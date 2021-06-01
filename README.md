## Service Locator Specification for Data Transfer

### About

This will describe the solution for a Service Locator solution for data transfer through our system.

### Problems:

* Data transfer cannot be lost
* Dynamic discovery of endpoints
* Endpoints must not be discoverable if they no longer exist
* Inter-host transfer of data must be allowed

### Procedure

#### Registration

Frame structure (Req):

1. 'register'
2. \<service-name\>
3. ipc://\<location\>

#### Query

Frame Structure (Req):

1. 'query'
2. \<service-name\>

Frame Structure (Rep):

1. \[repeated\] ipc://\<location\>