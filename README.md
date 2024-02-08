# Matrix-Multiplication-Using-Hadoop

Steps to execute Hadoop Program
1. Setup JAVA_HOME to system variables. link to setup JAVA_HOME https://www.wikihow.com/Set-Java-Home
2. Download Windows Subsystem for Linux (WSL), type 'wsl --install' to install this and then execute the below commands
   1-> sudo apt update
   2-> sudo apt upgrade
   3-> sudo apt install openjdk-8-jdk maven
4. Download Ubuntu
5. To download Hadoop, execute these commands
    1) cd
    2) wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.2/hadoop-3.3.2.tar.gz
    3) tar xfz hadoop-3.3.2.tar.gz
6. To execute Hadoop program, execute these commands in cmd
    1) go to folder in cmd and type 'mvn install'. If any compilation issue occurs resolve it, else go to next command
    2) rm -rf intermediate output
    3) ~/hadoop-3.3.2/bin/hadoop jar target/*.jar Multi M-matrix-small.txt N-matrix-small.txt intermediate output
    4) Resolve if any issue occurs, else a file named 'part-r-00000' will be created with output folder 
