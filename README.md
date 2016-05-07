# Description

This project is about Elasticsearch and how to use the Transport client and Jest REST client in Java. I used TestNG
as environment basically.

# Requirements

* A running Elasticsearch 2 instance, e.g. in a Vagrant box ala `https://github.com/sikron/vagrant-elasticsearch-simple`

# Parameters

* Host and port in the tests should be adapted

# Modules

* `pocs`
  Basic index-creation, uploading data and searching over REST and with Transport client via TestNG tests.
* `scenario`
  A little scenario with more encapsulated implementation. Also driven by TestNg.