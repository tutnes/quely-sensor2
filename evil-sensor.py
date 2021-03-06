#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ariot hackaton,
from scapy.all import *
from scapy.fields import ConditionalField
import struct
from datetime import datetime
import pprint
import requests
import json
from gcloud import pubsub
from os import system

# configuration
evil_endpoint = "http://relay-dev.westeurope.cloudapp.azure.com:8888/track"
node_name = "node-not-set"

PROJECT='practical-brace-126614'
TOPIC='Balle'
PROBE_REQUEST_TYPE=0
PROBE_REQUEST_SUBTYPE=4
interface_to_channel = {
    'wlan0': '1',
    'wlan1': '6',
    'wlan2': '11',
    'wlan3': '14'
}

already_seen = dict()

def patch_send():
    import httplib
    old_send= httplib.HTTPConnection.send
    def new_send( self, data ):
        print "----------------"
        print data
        print "================"
        return old_send(self, data) #return is not necessary, but never hurts, in case the library is changed
    httplib.HTTPConnection.send= new_send

def PacketHandler(pkt):
    global node_name
    if pkt.haslayer(Dot11):
        #if pkt.type==PROBE_REQUEST_TYPE and pkt.subtype == PROBE_REQUEST_SUBTYPE and ( pkt.addr2.lower() in WHITELIST or pkt.addr2.upper() in WHITELIST):
        if pkt.type==PROBE_REQUEST_TYPE and pkt.subtype == PROBE_REQUEST_SUBTYPE:
            SendPacket(pkt, node_name)
            PrintPacket(pkt)

def ConvertFromMhzToWifiChannel(mhz):
    """Crude, but works (only for 2.4ghz) which is all we use"""
    return min(14, max(1, (mhz - 2407) / 5))

def ParsePacket(pkt):
    radiotap_formats = {"TSFT":"Q", "Flags":"B", "Rate":"B",
      "Channel":"HH", "FHSS":"BB", "dBm_AntSignal":"b", "dBm_AntNoise":"b",
      "Lock_Quality":"H", "TX_Attenuation":"H", "dB_TX_Attenuation":"H",
      "dBm_TX_Power":"b", "Antenna":"B",  "dB_AntSignal":"B",
      "dB_AntNoise":"B", "b14":"H", "b15":"B", "b16":"B", "b17":"B", "b18":"B",
      "b19":"BBB", "b20":"LHBB", "b21":"HBBBBBH", "b22":"B", "b23":"B",
      "b24":"B", "b25":"B", "b26":"B", "b27":"B", "b28":"B", "b29":"B",
      "b30":"B", "Ext":"B"}

    
    values = {
        'timestamp': datetime.now().isoformat(),
        'target': pkt.addr3,
        'source': pkt.addr2,
        'SSID': pkt.getlayer(Dot11ProbeReq).info
    }
    if pkt.haslayer(Dot11):
        if pkt.addr2 is not None:
            field, val = pkt.getfield_and_val("present")
            names = [field.names[i][0] for i in range(len(field.names)) if (1 << i) & val != 0]
            fmt = "<"
            ChannelPos = None
            RatePos = None
            positions = {}
            for name in names:
                positions[name] = len(fmt)-1
                fmt = fmt + radiotap_formats[name]
            decoded = struct.unpack(fmt, pkt.notdecoded[:struct.calcsize(fmt)])
            
            for name in positions.keys():
                #print name + " = " + str(decoded[positions[name]])
                values[name] = decoded[positions[name]]
            if 'Channel' in values:
                values['ChannelNumber'] = ConvertFromMhzToWifiChannel(values['Channel'])
                #print "ChannelNumber = " + str(values['ChannelNumber'])
    return values
                

def PrintPacket(pkt):
    #print "Probe Request Captured:"
    info = ParsePacket(pkt)
    seen_flag = " "
    if pkt.addr2 not in already_seen:
        already_seen[pkt.addr2] = True
        seen_flag = "*"
    #print "[%s] %s Target: %s Source: %s SSID: %s RSSi: %d"%(info.timestamp, seen_flag, info.target, info.source, info.SSID, info.dBm_AntSignal)
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(info)

def SendPacket(pkt, node_name):
    payload = ParsePacket(pkt)
    payload['node_name'] = node_name
    #import json
    
    #r = requests.post(evil_endpoint, json = payload)
    #requests.post(evil_endpoint, data=json.dumps(payload), headers={"content-type": "text/javascript"})
    update(payload)


def update(payload):
    print(json.dumps(payload))
    topic.publish(json.dumps(payload))
    #print (topic.exists())
        #topic.create()  # API request
    #topic.publish("This is a test messag")

def setup_pubsub():
    global client
    global topic
    client = pubsub.Client(project=PROJECT)
    topic = client.topic(TOPIC)
    if not topic.exists():
        topic.create()



def setup_interface(iface):
    """Put the interface in monitor mode, and set channel, ugly way"""
    print "Setting %(iface)s to monitor mode" % { 'iface': iface }
    system("ifconfig %(iface)s down ; iwconfig %(iface)s mode monitor ; ifconfig %(iface)s up" % { 'iface': iface })
    if iface not in interface_to_channel:
        print "No channel found for %(iface)s, leaving at current channel" % { 'iface': iface }
        return
    channel = interface_to_channel[iface]
    print "Setting channel for %(iface)s to %(channel)s" % { 'iface': iface, 'channel': channel }
    system("iwconfig %(iface)s channel %(channel)s" % { 'iface': iface, 'channel': channel })

def set_node_name():
    """just use the mac address of eth0, or fallback to unknown"""
    global node_name
    f = open('/sys/class/net/eth0/address')
    lines = f.readlines()
    f.close()
    if len(lines) > 0:
        node_name = lines[0].strip()
            
def main():
    global node_name
    if len(sys.argv) < 2:
        print "Please specify evil network interface"
        sys.exit(1)
    if len(sys.argv) >= 3:
        node_name = sys.argv[2]
    evil_interface = sys.argv[1]

    print "[%s] EvilCorp starting sensor" % datetime.now()
    setup_interface(evil_interface)
    set_node_name()
    setup_pubsub()
    #patch_send() # useful for debugging
    sniff(iface=evil_interface, prn=PacketHandler, store=0)
    
if __name__=="__main__":
    main()
