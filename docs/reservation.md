---
layout: documentation
---

# Reservation

Mesos 0.23.0 introduces __dynamic reservation__ which enables operators and
frameworks to dynamically partition a Mesos cluster. There are no breaking
changes being introduced with dynamic reservations, which means the existing
static reservation mechanism continues to be fully supported.

Resources can be reserved for a __role__ (same as static reservation), and can
be managed by either a Mesos cluster __operator__ or a __framework__.

* `/reserve` and `/unreserve` HTTP endpoints are available for Mesos cluster
  __operators__ to manage the reservations through the master.
* `Offer::Operation::Reserve` and `Offer::Operation::Unreserve` messages are
  available for __frameworks__ to send back via the `acceptOffers` API as a
  response to a resource offer.

The following sections, we'll first cover the existing static reservation
mechanism, then describe the basic usage followed by the advanced usage of the
new reservation mechanisms.

## Static Reservation (existed prior to 0.23.0)

The Mesos cluster operator can configure a slave with resources reserved for a
role. The reserved resources are specified via the `--resources` flag.
For example, suppose we have 12 CPUs and 6144 MB of RAM available on a slave and
that we want to reserve 8 CPUs and 4096 MB of RAM for the `ads` role.
We start the slave like so:

```bash
$ ./mesos-slave --master=<ip>:<port>
                --resources="cpus:4;mem:2048;cpus(ads):8;mem(ads):4096"
```

We now have 8 CPUs and 4096 MB of RAM reserved for `ads` on this slave, and this
configuration is not modifiable without draining and restarting slave.

> __NOTE__: This feature is supported for backwards compatibility.
>           The recommended approach is to specify the total resources available
>           on the slave as unreserved via the `--resources` flag and manage
>           reservations dynamically via the master HTTP endpoints.

## Dynamic Reservation

As mentioned in [Static Reservation](#static-reservation), specifying the
reserved resources via the `--resources` flag makes the reservation static.
This means that statically reserved resources cannot be reserved for another
role nor be unreserved. Dynamic Reservation enables an operator or framework to
effectively further partition the unreserved portion of the slave post startup.
An __operator__ performs dynamic reservations via `/reserve` and `/unreserve`
HTTP endpoints on the Mesos master, and a __framework__ sends back
`Offer::Operation::Reserve` and `Offer::Operation::Unreserve` offer operations
via the `acceptOffers` API.

In the following sections, we'll walk through examples of the 4 interfaces:
  * `/reserve`
  * `/unreserve`
  * `Offer::Operation::Reserve`
  * `Offer::Operation::Unreserve`

### `/reserve`

Suppose we want to reserve 8 CPUs and 4096 MB of RAM for the `ads` role on
a slave with id=`<slave_id>`. We send an HTTP POST request to the `/reserve`
HTTP endpoint like so:

```bash
$ curl -d slaveId=<slave_id>
       -d resources="{
         {
           "name" : "cpus",
           "type" : "SCALAR",
           "scalar" : { "value" : 8 },
           "role" : "ads",
           "reservation" : {
             "principal" : "ops"
           }
         },
         {
           "name" : "mem",
           "type" : "SCALAR",
           "scalar" : { "value" : 4096 },
           "role" : "ads",
           "reservation" : {
             "principal" : "ops"
           }
         },
       }"
       -X POST http://<ip>:<port>/master/reserve
```

Mesos will attempt to reserve the resources by converting unreserved resources
to reserved resources. Suppose Mesos has 12 CPUs and 6144 MB of RAM unreserved
and not currently being used. If the reserve operation succeeds, we end up with
8 CPUs and 4096 MB of RAM reserved for role `ads` and 4 CPUs and 2048 MB of RAM
is left unreserved. If the reserve operation fails, the user receives a
`Reserve Operation Failed` HTTP response.

### `/unreserve`

Suppose we want to unreserve the resources that we dynamically reserved above.
We can send an HTTP POST request to the `/unreserve` HTTP endpoint like so:

```bash
$ curl -d slaveId=<slave_id>
       -d resources="{
         {
           "name" : "cpus",
           "type" : "SCALAR",
           "scalar" : { "value" : 8 },
           "role" : "ads",
           "reservation" : {
             "principal" : "ops"
           }
         },
         {
           "name" : "mem",
           "type" : "SCALAR",
           "scalar" : { "value" : 4096 },
           "role" : "ads",
           "reservation" : {
             "principal" : "ops"
           }
         },
       }"
       -X POST http://<ip>:<port>/master/unreserve
```

Mesos will attempt to unreserve the resources by converting the reserved
resources to unreserved resources. Suppose Mesos has 8 CPUs and 4096 MB of RAM
reserved for role `ads` and not currently being used, and 4 CPUs and 2048 MB of
RAM unreserved. If the unreserve operation succeeds, we end up with 12 CPUs and
6144 MB of RAM unreserved. If the unreserve operation fails, the user receives
a `Unreserve Operation Failed` HTTP response.

### Offer::Operation::Reserve

Suppose we receive a resource offer with 12 CPUs and 6144 MB of RAM unreserved.

```
{
  "id" : <offer_id>,
  "framework_id" : <framework_id>,
  "slave_id" : <slave_id>,
  "hostname" : <hostname>,
  "resources" : [
    {
      "name" : "cpus",
      "type" : "SCALAR",
      "scalar" : { "value" : 12 },
      "role" : "*",
    },
    {
      "name" : "mem",
      "type" : "SCALAR",
      "scalar" : { "value" : 6144 },
      "role" : "*",
    }
  ]
}
```

We can reserve 8 CPUs and 4096 MB of RAM by sending the following
`Offer::Operation` message. `Offer::Operation::Reserve` has a `resources` field
which we fill in with the desired state of the resources.

```
[
  {
    "type" : Offer::Operation::RESERVE,
    "resources" : [
      {
        "name" : "cpus",
        "type" : "SCALAR",
        "scalar" : { "value" : 8 },
        "role" : <framework_role>,
        "reservation" : {
          "principal" : <framework_principal>
        }
      }
      {
        "name" : "mem",
        "type" : "SCALAR",
        "scalar" : { "value" : 4096 },
        "role" : <framework_role>,
        "reservation" : {
          "principal" : <framework_principal>
        }
      }
    ]
  }
]
```

In the subsequent resource offer, we will receive an resource offer like this:

```
{
  "id" : <offer_id>,
  "framework_id" : <framework_id>,
  "slave_id" : <slave_id>,
  "hostname" : <hostname>,
  "resources" : [
    {
      "name" : "cpus",
      "type" : "SCALAR",
      "scalar" : { "value" : 8 },
      "role" : <framework_role>,
      "reservation" : {
        "principal" : <framework_principal>
      }
    },
    {
      "name" : "mem",
      "type" : "SCALAR",
      "scalar" : { "value" : 4096 },
      "role" : <framework_role>,
      "reservation" : {
        "principal" : <framework_principal>
      }
    },
    {
      "name" : "cpus",
      "type" : "SCALAR",
      "scalar" : { "value" : 4 },
      "role" : "*",
    },
    {
      "name" : "mem",
      "type" : "SCALAR",
      "scalar" : { "value" : 2048 },
      "role" : "*",
    }
  ]
}
```

### Offer::Operation::Unreserve

A framework is able to unreserve resources through the resource offer cycle.
Suppose we receive a resource offer with 8 CPUs and 4096 MB of RAM reserved and
the 4 CPUs and 2048 MB of RAM unreserved:

```
{
  "id" : <offer_id>,
  "framework_id" : <framework_id>,
  "slave_id" : <slave_id>,
  "hostname" : <hostname>,
  "resources" : [
    {
      "name" : "cpus",
      "type" : "SCALAR",
      "scalar" : { "value" : 8 },
      "role" : <framework_role>,
      "reservation" : {
        "principal" : <framework_principal>
      }
    },
    {
      "name" : "mem",
      "type" : "SCALAR",
      "scalar" : { "value" : 4096 },
      "role" : <framework_role>,
      "reservation" : {
        "principal" : <framework_principal>
      }
    },
    {
      "name" : "cpus",
      "type" : "SCALAR",
      "scalar" : { "value" : 4 },
      "role" : "*",
    },
    {
      "name" : "mem",
      "type" : "SCALAR",
      "scalar" : { "value" : 2048 },
      "role" : "*",
    }
  ]
}
```

and we can unreserve the 8 CPUs and 4096 MB of RAM by sending the following
`Offer::Operation` message. `Offer::Operation::Unreserve` has a `resources` field
which we fill in with the resources that we want to unreserve.

```
[
  {
    "type" : Offer::Operation::UNRESERVE,
    "resources" : [
      {
        "name" : "cpus",
        "type" : "SCALAR",
        "scalar" : { "value" : 8 },
        "role" : <framework_role>,
        "reservation" : {
          "principal" : <framework_principal>
        }
      }
      {
        "name" : "mem",
        "type" : "SCALAR",
        "scalar" : { "value" : 4096 },
        "role" : <framework_role>,
        "reservation" : {
          "principal" : <framework_principal>
        }
      }
    ]
  }
]
```

In the subsequent resource offer, we will receive an resource offer like this:

```
{
  "id" : <offer_id>,
  "framework_id" : <framework_id>,
  "slave_id" : <slave_id>,
  "hostname" : <hostname>,
  "resources" : [
    {
      "name" : "cpus",
      "type" : "SCALAR",
      "scalar" : { "value" : 12 },
      "role" : "*",
    },
    {
      "name" : "mem",
      "type" : "SCALAR",
      "scalar" : { "value" : 6144 },
      "role" : "*",
    }
  ]
}
```
