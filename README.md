# Data Service Locator Specification for Data Transfer

## About

This will describe the solution for a Service Locator solution for data transfer through our system.

## Goals:

* Data transfer cannot be lost (unless deliberately allowed)
* Dynamic discovery of endpoints
* Endpoints must not be discoverable if they no longer exist
* Inter-host transfer of data must be allowed
* New instances of applications must be able to be seamlessly introduced

## Locator vs Discovery?

We are not going to adopt Discovery for now. Discovery would require far more engineering and effort than is necessary
for our simple system of limited hosts and applications. The main goal here is to reduce the static configuration
required for our applications and allow the creation of more interesting tools to assist developers in delivering good
quality features and code.

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

## How many Service Locators to deploy?

It is my intent to only deploy a single Service Locator on the controller host that every service can submit queries and
registration requests. This makes the most sense for our system as a starting point as it is dead simple and extremely
high uptimes is unnecessary. We can incur the cost of a full system restart for example and that should address any full
system errors that might happen as a result of this design. In addition, should we need something more robust to server
errors, we can also deploy a secondary fail-over ServiceLocator using the described model in the ZMQ guide. This flat
out assumes that the rest of the system can handle the other applications on the controller host also dying as a result
of the controller host suffering issues. Obviously, this is unrealistic.

## Procedure

### Registration

Request:

1. 'register'
2. \<service-name\>
3. tcp://\<service-location\>

### Force

Request:

1. 'force-query'
2. \<service-name\>

Response:

1. \[repeated\] tcp://\<service-location\>

### Query

Request:

1. 'query'
2. \<service-name\>

Response:

1. \[repeated\] tcp://\<service-location\>

### Deregister

Request:

1. 'deregister'
2. \<service-name\>
3. tcp://\<service-location\>

## Restrictions

* Writers perform the bind
* Readers expect to have to connect and read data from multiple serviceLocations
* All data transfer sockets are PUSH/PULL
* (Optional) support for non-blocking PUSH/PULL sockets
* (Optional) support for flipping between Writers/Readers being bind or connect.

## Bugs & Limitations

* There currently exists a problem where if the reader disconnects and then reconnects too quickly before the DSL has
  time to timeout the old pending request, then the DSL will think that it has already sent back a report and if there
  are no changes, then the reader will not get a response back. We can do one of two fixes for this:
    * We can detect disconnections and expire out the pending request for that socket.
    * We can add a new command to force a query which clients utilize on first lookup.
* Another interesting problem (and annoying) is that the address that we're binding to is different to the address that
  readers have to connect to. For instance, when binding on `tcp://*:19275`, you have to provide to the DSL 
  `tcp://<hostname>:19275` so I've had to write a weird parse for string urls into it. 