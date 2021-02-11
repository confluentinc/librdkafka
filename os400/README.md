librdkafka - port to IBM i OS
==================================================

This document contains instructions for building and testing the **librdkafka** on the IBM i platform.

# Building **librdkafka** on IBM i

## Prerequisites
Required runtime object is inclded into OS400 - service program QSYS/QADRTTS. To be able to compile librdkafka on IBM i server, 
it is necessary to install [ASCII Runtime for IBM i](https://www.ibm.com/support/pages/node/6258183) on your IBM i server.

The preferred tool for building librdkafka on IBM i is gnu gmake, included in IBM Tools for Developers for IBM i (5799PTL). 
If you do not have this product installed, you can build the library using a QShell script that executes standard commands CRTCMOD, CRTSRVPGM, CRTPGM

### Build steps using GNU utilites

### Build steps using simple qshell script

# Compiled binaries

# How to run standard librdkafka tests

You will need access to the kafka cluster to run the tests. You can use an external cluster for this or start a cluster on your server. 

## How to run a test kafka cluster on your server 



TODO
