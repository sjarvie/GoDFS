# Go DFS

Go DFS is a distributed filesystem inspired by the GoogleFS and Hadoop projects. The project utilizes a Namenode process to manage File and Block metadata, in order to manage the files across many Datanodes. A client submits requests to the Namenode to insert or retrieve files from the system.


### Installation

Using Go 1.2, run go install on each package and the main level directory. This will create the godfs executable.


### Running

Run the following in serparate processes

* Start the namenode

		sjarvie/GoDFS$ godfs namenode
	
* Start a datanode
godfs datanode [id] [local absolute path to store blocks]

		sjarvie/GoDFS$ godfs datanode DN1 /Data1

* Start the client

		sjarvie/GoDFS$ godfs client
	
	
In the client proccess, the following commands can be issued to save or retrieve files

* Insert a file

  put [local file absolute path] [desired remote path]
  
* Retrieve a file

get [remote path to retrieve file from] [local absolute path to save file]

	

  	put /home/sjarvie/file.txt /file.txt
  
  	get /file.txt /home/sjarvie/out.txt
  
