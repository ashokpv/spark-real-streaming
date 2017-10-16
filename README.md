#JAVA 8 INSTALL#
	sudo add-apt-repository ppa:webupd8team/java
	sudo apt-get update
	sudo apt-get install oracle-java8-installer
	sudo apt-get install oracle-java8-set-default


# HADOOP DOWNLOAD AND INSTALL #

	wget http://apache.rediris.es/hadoop/common/hadoop-2.7.0/hadoop-2.7.0.tar.gz
	sudo tar -xzvf hadoop-2.7.0.tar.gz -C /usr/local/lib/
	sudo chown -R hadoop:hadoop /usr/local/lib/hadoop-2.7.0
	
	sudo mkdir -p /var/lib/hadoop/hdfs/namenode
	sudo mkdir -p /var/lib/hadoop/hdfs/datanode
	sudo chown -R hadoop /var/lib/hadoop

CONGIGURE HADOOP FILES FROM THE LINK "http://thepowerofdata.io/setting-up-a-apache-hadoop-2-7-single-node-on-ubuntu-14-04/"

#DOWNLOAD SPARK AND INSTALL ##
DOWNLOAD THE TAR FILE FROM : https://spark.apache.org/downloads.html

	tar xvf spark-1.3.1-bin-hadoop2.6.tgz 
	mv spark-1.3.1-bin-hadoop2.6 /usr/local/spark 
	
	export PATH = $PATH:/usr/local/spark/bin
	
	source ~/.bashrc

TO CHECK ITS WORKING RUN THE COMMANDS

	spark-shell

FOR CHECKING PYSPARK INSTALLATION

	pyspark


### Set up ###

* Installation
    * Dependencies for SciPy module `apt-get install libblas-dev liblapack-dev libatlas-base-dev gfortran`
    

* Change the python path in .bashrc
	
    * export PYSPARK_PYTHON=/usr/bin/python3

#INSTALL PYTHON PACKAGES

    * pip3 install pandas NumPy Scipy scikit-learn pymongo
    
#INSTALL MONGO

	sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
	echo "deb http://repo.mongodb.org/apt/ubuntu trusty/mongodb-org/3.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.0.list
	sudo apt-get update
	sudo apt-get install -y mongodb-org
	* Verify installation
		sudo service mongod start
