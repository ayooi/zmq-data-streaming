# Data Service Locator Specification for Data Transfer

## About

This will describe the solution for a Service Locator solution for data transfer through our system.

## Goals:

* Data transfer cannot be lost (unless deliberately allowed)
* Dynamic discovery of endpoints
* Endpoints must not be discoverable if they no longer exist
* Inter-host transfer of data must be allowed
* New instances of applications must be able to be seamlessly introduced

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

#### Advantages

* Simple to understand
* Single point to debug if anything is amiss
* Easy to deploy

#### Disadvantages

* Single point of failure
* Requires clients to regularly request for updates
* Updates are applied slowly as clients need to poll
* No ability to keep data flowing across services on a local host. All data may be consumed by any host within the
  cluster

#### Amendments

``Updates are applied slowly as clients need to poll`` and ``Requires clients to regularly request for updates``

This can be mitigated to some degree by using a DEALER-ROUTER combination between the client and the DSL. The DSL can
immediately after there has been a change in endpoints. The client will send requests with a timeout to account for if
the DSL has died.

``No ability to keep data flowing across services on a local host``

This can be addressed by qualifying registered applications with their hostname. This allows the client to make a
determination within application level code whether it should connect to all available endpoints or only those on its
immediate host. Another possibility might be simply to reserve ```tcp://``` endpoints for inter-host distribution
and ```ipc://``` for local only distribution

To begin with, we should probably only support ```tcp://```. There are two reasons for this.

1. JeroMQ doesn't support ```ipc://``` so this will make Java and C++ inter-application communication impossible
2. If that isn't a good enough reason, it would also complicate things that I just don't want to deal with at this point
   in time

## System Centralized vs. Distributed Host Centralized vs. True Decentralized

An important question to answer is whether we should do a single DSL on the common host or something more distributed.
We have two other options. Breaking up the DSL into an instance for each host or doing a true client side service
discovery. We're pretty familiar with the concept of a System Centralized DSL so let's explore the other options:

### Distributed Host Centralized

* Each host has its own DSL deployed to it
* Each DSL has to know of each other and share state.
* TBC

## Procedure

### Registration

Frame structure (Req):

1. 'register'
2. \<service-name\>
3. ipc://\<serviceLocation\>

### Query

Frame Structure (Req):

1. 'query'
2. \<service-name\>

Frame Structure (Rep):

1. \[repeated\] ipc://\<serviceLocation\>

## Restrictions

* Writers perform the bind
* Readers expect to have to connect and read data from multiple serviceLocations
* All data transfer sockets are PUSH/PULL
* (Optional) support for non-blocking PUSH/PULL sockets
* (Optional) support for flipping between Writers/Readers being bind or connect. 

