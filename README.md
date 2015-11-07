# sc-crud-rethink
Realtime CRUD data management layer/plugin for SocketCluster using RethinkDB as the database.

See https://github.com/socketcluster/sc-sample-inventory for a full working sample.

## Setup

See https://github.com/socketcluster/sc-sample-inventory for sample app which demonstrates this component in action.

This module is a plugin for SocketCluster, so you need to have SC installed: http://socketcluster.io/#!/docs/getting-started
Once SC is installed and you have created a new SC project, you should navigate to your project's main directory and run:

```bash
npm install sc-crud-rethink --save
```

Now you will need to attach the plugin to your worker - So open ```worker.js``` and attach it to your worker instance like this:
https://github.com/SocketCluster/sc-sample-inventory/blob/master/worker.js#L27-L81

As shown in the sample above, you will need to provide a schema for your data.
In the example above, the Category, Product, and User keys represent tables/models within RethinkDB - Inside each of these, you
need to declare what fields are allowed and optionally the **views** which are supported for each model type.

Simply put, a **view** is an ordered, filtered subset of all documents within a table. Views need to define a ```filter``` and/or ```order``` function
which will be used to construct the view for table's data.

As you can see here: https://github.com/SocketCluster/sc-sample-inventory/blob/master/worker.js#L58-L63 - The filter and order functions are responsible for
creating predicates which will be used by RethinkDB to filter and order data. Both the filter and order functions accept optional predicateData as an argument.
The client/frontend of your app is responsible for providing that predicate data. See https://github.com/SocketCluster/sc-collection for more info about predicate data.
