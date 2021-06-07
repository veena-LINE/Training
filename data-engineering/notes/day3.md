DFS



File Systems

  FAT
  FAT32
  NTFS
  
  ..
  XFS
  ETX4
  ....
  
 
 4 TB - MAX size of HDD
 
 Data File - 10 TB size
 
 Failure, what if HDD failed
 
 -----------------
 
 Servers - Dell Server
 
 MAX Server storage capacity - 16 TB
 
 Data Files consist of 100 TB
 
 what happen when the server failed
 
 --------
 
 Solution: DFS => Distributed File System
 
 Use more than one system to store the data
 
 
10 TB file 
 	Split the file content into chunk / block
		chunk/block 0  - 64 MB
		chunk/block 1  - 64 MB
		.....
		
		....
		chunk/block n - 64 MB
		
		..
		...
		chunk/block m - 64 MB
		
					sum of all block - 10 TB
 
 
 3 systems/nodes/computers/vms
 
 
 Distribute the 10 TB of data (blocks of 64 MB size)
 	 into 3 systems...
 
 
 Data Node 1 - HDD 2 TB
 			block 0, block 1....block 100
			block 10000, 100001... 1000100
			
			B1000 - Replication 3 factor - copy 1
			..
 Data Node 2 - HDD 8 TB
 			block 200, block 1....block 300
			 
			B1000 - Replication 3 factor  - copy 2
			..
 Data Node 3 - HDD 16 TB
 			block 300, block 1....block 400
			 B1000 - Replication 3 factor  - copy 3

 Data node 4 - 20 TB
 
 ....
 ....
 
 ## replication?
 
 	Storing the same data in multiple copies
				 B0      store in 3 nodes instead of 1 ndoe


			 
  Name Node
  			meta data
				directory name
				 file name 
				 	block number on the order
						pointing to data nodes
						
					B0 - Datanode 1, disk position... size
					B1 - Datanode 2, disk position... size
					B3 - Datanode 1
					...
					..
					
					
  			
			..
 ...
 ...
 ..
 
 Program
 
 	read a file from DFS
		issue 1: data stored in to different node
		issue 2: we need the data in ordered fashion
		issue 3: performance of reading all 10 TB data
 		issue 4: what if data node 1 fails
		 
