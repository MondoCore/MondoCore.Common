# MondoCore.Common
  General purpose classes and interfaces used by other MondoCore libraries. 

## Table of Contents
- [Interfaces](#interfaces)
    - [IBlobStore\<T\>](#IBlobStore)
    - [IBlobStoreReader\<T\>](#IBlobStoreReader)
    - [IBlobStoreWriter\<T\>](#IBlobStoreWriter)
    - [ICache](#ICache)
    - [IMessageQueue\<T\>](#IMessageQueue)
    - [IMessageQueueFactory\<T\>](#IMessageQueueFactory)
- [Extensions](#extensions)
    - [IBlobStoreReader\<T\>](#IBlobStoreReaderEx)
    - [IBlobStoreWriter\<T\>](#IBlobStoreWriterEx)
    - [IEnumerable\<T\>](#IEnumerableEx)
- [Classes](#classes)
    - [FileStore](#FileStore)
    - [MemoryCache](#MemoryCache)
    - [MemoryStore](#MemoryStore)

<a href="interfaces" />

<br/>

## Interfaces

### IBlobStore

> Interface for accessing blobs/files.

#### Implementations

* FileStore
* MemoryStore

<br>

___
<a href="IBlobStore" />

### IBlobStore\<T\>

> Interface for accessing blobs/files.

##
<small>IBlobStoreReader\<T\></small> <b>Reader</b>

> Returns an IBlobStoreReader interface to read blobs from the store

##
<small>IBlobStoreWriter\<T\></small> <b>Writer</b>

> Returns an IBlobStoreWriter interface to write blobs to the store

#### Implementations

* AzureBlobStorage\<T\> (in [MondoCore.Azure.Storage](https://github.com/MondoCore/MondoCore.Azure.Storage))
* AzureAppendBlobStorage\<T\> (in [MondoCore.Azure.Storage](https://github.com/MondoCore/MondoCore.Azure.Storage))
* AzurePageBlobStorage\<T\> (in [MondoCore.Azure.Storage](https://github.com/MondoCore/MondoCore.Azure.Storage))

<br>

___
<a href="IBlobStoreReader" />

### IBlobStoreReader\<T\>

> Interface for reading blobs/files.

##
<small>Task</small> <b>Get</b>(<small>string id, Stream destination, CancellationToken cancellationToken = default</small>)

> Loads a blob from the store.

##
<small>Task<Stream></small> <b>OpenRead</b>(<small>string id, CancellationToken cancellationToken = default</small>)

> Returns a stream that the blob can be read from. Be sure to dispose of the stream using DisposeAsync (i.e. await using)

##
<small>Task<IEnumerable<string>></small> <b>Find</b>(<small>string filter, Stream destination, CancellationToken cancellationToken = default</small>)

> Finds all blobs that meet the filter. The filter uses standard "*.*" file name (id) matching. The matching filename/ids are returned.

##
<small>Task</small> <b>Enumerate</b>(<small>string filter, Func<IBlob, Task> fnEach, bool asynchronous = true, CancellationToken cancellationToken = default</small>)

> Finds all blobs that meet the filter and enumerates over the that list using the given Function. The filter uses standard "*.*" file name (id) matching.

<br/>

___
<a href="IBlobStoreWriter" />

### IBlobStoreWriter\<T\>

> Interface for writing blobs/files.

___
<a href="ICache" />

### ICache

> Interface for caching data.

#### Implementations

* MemoryCache
<br>

<a href="IMessageQueue" />

### IMessageQueue\<T\>

> Interface for sending messages

##
<small>async Task</small> <b>Send</b>(<small>T message, DateTimeOffset? sendOn = null, string? correlationId = null, CancellationToken cancellationToken = default</small>)

> Sends a message to the queue with an optional date/time offset.

#### Implementations

* ServiceBusQueue<T> (in [MondoCore.Azure.ServiceBus](https://github.com/MondoCore/MondoCore.Azure.ServiceBus))
* AzureStorageQueue<T> (in [MondoCore.Azure.Storage.Queue](https://github.com/MondoCore/MondoCore.Azure.Storage.Queue))

<a href="IMessageQueueFactory" />

### IMessageQueueFactory\<T\>

> Interface for creating message queues on the fly

##
<small>async Task</small> <b>CreateQueue</b>(<small>string queueName</small>)

> Creates a message queue (doesn't physically create the queue just the object to access it).


<br>

<a href="extensions" />

## Extensions

<a href="IBlobStoreReaderEx" />

### IBlobStoreReader\<T\>

##
<small>async Task\<T\></small> <b>GetObject</b>(<small>CancellationToken cancellationToken = default</small>)

> Loads an object from a json blob

___
<a href="IBlobStoreWriterEx" />

### IBlobStoreWriter\<T\>

##
<small>async Task</small><b> PutObject</b>(<small>T obj, CancellationToken cancellationToken = default</small>)

> Saves an object as a json blob

___
<a href="IEnumerableEx" />

### IEnumerable\<T\>

##
<small>async Task</small> <b>ParallelForEach</b>(<small>Func<long, T, Task> fnEach, int maxParallelism = 128, CancellationToken cancelToken = default</small>)

Iterates over a list of objects in parallel and runs a function on each one.

<br>

<a href="classes" />

## Classes

### FileStore
> Implements the blob store interface over the local file system using the [IBlobStore](#IBlobStore) interface

___
### MemoryCache
> Implements an in memory cache using the [ICache](#ICache) interface

___
### MemoryStore
> Implements an in memory blob store using the [IBlobStore](#IBlobStore) interface

___
License


> MIT
