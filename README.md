# Go DFS

Go DFS is a distributed filesystem inspired by the GoogleFS and Hadoop projects. The project utilizes a Namenode process to manage File and Block metadata, in order to manage the files across many Datanodes. Files are broken into Blocks whose headers contain the nexecessary metadata in order to decontruct, distribute, and reconstruct the data contents. 

The Namenode process constructs a dynamic filesystem by merging Block metadata provided by Datanodes as they connect to the system. A client submits requests to the Namenode to insert or retrieve files from the datanodes, which manage Block level writes and retrieval. Many datanodes can be added or removed on demand, granting a scalable data model for future projects which would like to improve upon this design.


### Installation

Using Go version >= 1.2[(download)](https://www.google.com), run go install on each package and the main level directory. This will create the godfs executable.


### Usage

* Start up processses

	`godfs namenode [configuration location]`

	`godfs datanode [configuration location]`
	
	`godfs client [configuration location]`


* Insert a file :   
 
	`put [local file absolute path] [desired remote path]`
  
* Retrieve a file :  

	`get [remotepath] [localpath]`

* List remote filesystem contents

	`list`



### Example


Run the following in serparate processes from the top level directory:

* Start the namenode

	`sjarvie/GoDFS$ godfs namenode namenode/examplenamenode.xml`
	
* Start a datanode :

	`sjarvie/GoDFS$ godfs datanode datanode/exampledatanode.xml`

* Start the client

	`sjarvie/GoDFS$ godfs client client/exampleclient.xml`
	
* Insert a file :   
 
  	`put /home/sjarvie/localfile.txt /remotefile.txt`

  
* Retrieve a file :  

	`get /remotefile.txt /home/sjarvie/out.txt`
  
### Testing

In each directory (client, datanode, namenode, toplevel) run

	`go test`


