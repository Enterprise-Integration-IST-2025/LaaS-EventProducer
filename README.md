# LaaSSimulator
IE 2024 tool to act as an event producer for LaaS, using a Kafka Producer 

The tool starts by discovering all the topic available in the kafka cluster and then randomize messages for all that discovered topics.
You are free to customize the tool accordingly with your needs. If so, create a pull request to change this repository.

Verify if JAVA 17 is available using the command: 

```
java -version
```

Then, to execute the generator of messages use the following command from the target directory:
```
java -jar LaaSSimulator.jar 
```
```
The usage of the Message Producer for LaaS 2024, for Enterprise Integration 2024 course, is the following.

LaaSSimulator --broker-list <brokers> --throughput <value> --typeMessage <value> --filterprefix <value> 
where, 
--broker-list: is a broker list with ports (e.g.: kafka02.example.com:9092,kafka03.example.com:9092), default value is localhost:9092
--throughput: is the approximate maximum messages to be produced by minute, default value is 10
--typeMessage: is the type of message to be produced: JSON or XML, default value is JSON
--filterprefix: is the prefix to be filtered. Only the topics starting with this prefix will be considered to sending messages.
```

One example of an Purchase_Event message sent, in JSON, to the dicovered topic in Kafka is:
```
{"Purchase_Event":
	{
		"TimeStamp":"2024-02-09 10:44:07.748",
		"LoyaltyCard_ID":"qjfif783",
		"Price":"3.21",
		"Product":"Pie",
		"Supplier":"Feathered Friends Pet Haven",
		"Shop":"ArcoCegoLisbon"
	}
}
```