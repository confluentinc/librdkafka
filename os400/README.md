librdkafka - port to IBM i OS
==================================================

This document contains instructions for building, testing and using the **librdkafka** on the IBM i platform.

# **librdkafka** on IBM i
This project contains the source code and compiled binaries of the librdkafka library on the IBM i platform. 
The supplied library RDKAFKA is *SRVPGM object that are compiled on the V7R2M0 version of IBM i, storage model=*SNGLVL, data model=*P128.
The service program uses ASCII Runtime for IBM i (QADRT). Thus, all text parameters of the service program functions are accepted and returned in ASCII coding. Programs that use this *SRVPGM, must convert values from/to EBCDIC themselves. For this purpose it is convenient to use the standard QADRT functions like QadrtConvertA2E and QadrtConvertE2A. 


## Prerequisites
Required runtime object is inclded into OS400 - service program QSYS/QADRTTS. To be able to compile librdkafka on IBM i server, 
it is necessary to install [ASCII Runtime for IBM i](https://www.ibm.com/support/pages/node/6258183) on your IBM i server.

The preferred tool for building librdkafka on IBM i is gnu gmake, It was included in IBM Tools for Developers for IBM i (5799PTL).
If you do not have this product installed, you can build the library using a QShell script that executes standard commands CRTCMOD, CRTSRVPGM, CRTPGM (see below)

### Build steps using GNU utilites
TODO

### Build steps using QShell script
TODO

# Compiled binaries
TODO

# How to run standard librdkafka tests
You will need access to the kafka cluster to run the tests. You can use an external cluster for this or start a cluster on your server. 
TODO

## How to run a test kafka cluster on your IBM i server 
TODO

