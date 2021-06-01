# Data Service Locator Specification for Data Transfer

## About

This will describe the solution for a Service Locator solution for data transfer through our system.

## Problems:

* Data transfer cannot be lost
* Dynamic discovery of endpoints
* Endpoints must not be discoverable if they no longer exist
* Inter-host transfer of data must be allowed

## Centralized vs Decentralized?

For our purposes is a centralized data service locator sufficient?

Let's try to quantify anything good or bad that we can think of.

Firstly, describe a simplest as possible scenario to get us started.

### Simple

#### Description

* Centralized Service Locator on the common host
* All applications that want to utilise it gets a statically configured endpoint provided at configuration time via
  ansible.
* All applications register themselves to the Data Service Locator
* All applications regularly update their registration to the Data Service Locator
* Data Service Locator removes any endpoints that have not been updated after a predefined timeout period.
* Any application can query the Data Service Locator for at list of endpoints
* Any reader application that has queried the DSL for a list of endpoints can regularly query for an up to date list of
  endpoint

#### Advantages

* Simple to understand
* Single point to debug if anything is amiss
* Easy to deploy

#### Disadvantages

* Single point of failure across the system
* Requires clients to regularly request for updates
* Updates are applied slowly as clients need to poll
* No ability to keep data flowing across services on a local host. All data may be consumed by any host within the
  cluster

#### Amendments

``Updates are applied slowly as clients need to poll`` and ``Requires clients to regularly request for updates``

This can be addressed by using a REQ-ROUTER combination between the client and the DSL. The client can send a request
across a REQ socket to the DSL which can send a response back in two cases:

* Immediately after there has been a change in the endpoints requested
* Periodically if there has not been a change in some amount of time as a heartbeat so that the applications know if the
  DSL has died

``No ability to keep data flowing across services on a local host``

This can be addressed by qualifying registered applications with their hostname. This allows the client to make a
determination within application level code whether it should connect to all available endpoints or only those on its
immediate host. Another possibility might be simply to reserve ```tcp://``` endpoints for inter-host distribution
and ```ipc://``` for local only distribution

## Procedure

### Registration

Frame structure (Req):

1. 'register'
2. \<service-name\>
3. ipc://\<location\>

### Query

Frame Structure (Req):

1. 'query'
2. \<service-name\>

Frame Structure (Rep):

1. \[repeated\] ipc://\<location\>

## Restrictions

* Writers perform the bind
* Readers expect to have to connect and read data from multiple locations
* All data transfer sockets are PUSH/PULL
* (Optional) support for non-blocking PUSH/PULL sockets


