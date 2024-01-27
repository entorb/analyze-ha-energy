#!/bin/bash

echo login
ssh pi@raspi3 'sudo cp /var/lib/docker/overlay2/08cae025466b8c222a2f13729424e9e8e891f271c36d8488473d0ddf9be1d18b/diff/config/home-assistant_v2.db /tmp; sudo chown pi:pi /tmp/home-assistant_v2.db'

echo copy
scp pi@raspi3:/tmp/home-assistant_v2.db ./

echo cleanup
ssh pi@raspi3 'sudo rm /tmp/home-assistant_v2.db'
