##EMR Cost Calculator

####A simple python module that calculates the cost of a single or a group of EMR clusters.

Given that Amazon doesn’t provide a straightforward solution to calculate the cost of an EMR workflow, this module aims to calculate the cost of an EMR workflow given a period of days,
or the cost of a single cluster given the cluster id. The simple way to do that would be to use the information given by the JobFLow method of the boto.emr module. However, this method 
doesn’t return any information about the Task nodes of a cluster, and whether or not spot instances were used. 

This cost calculator takes care of both. In case spot instances were used, the spot_price is retrieved by the corresponding field of the InstanceGroup emrobject. This figure corersponds 
to the bidprice and this is the price we use in order to calculate the total cost. In many cases however (and depending on your bidding policy) this might not be the final price that you 
pay for the instances (for more information about the amazon spot instance market check [here] (http://aws.amazon.com/ec2/purchasing-options/spot-instances/)). Since the price that we pay 
is the current minimum bidprice (that has been granted instances) a more accurate approach would require to monitor the minimum bid price while the cluster is alive and adjust the cluster cost 
accordingly. However, this approach adds a lot of complexity and in case that your bidding prices are not extremely higher than the minimum bid price, our current approach is accurate enough.

###How it works

In order for this module to work you need to have your EC2 key value pair stored in your .bashrc file so that boto can connect to the AWS EMR API.

This module is using [docopt](http://docopt.org/) in order to parse command line arguments.

It currently support two operations:
1. Get the total cost of an EMR worfklow for a given period of days
  * `emr_cost_calculator.py total --region=<The region you launched your clusters in> --created_after=<YYYY-MM-DD> --created_before=<YYYY-MM-DD>`
2. Get the cost of an EMR cluster given the cluster id
  * `emr_cost_calculator.py cluster --region=<The region you launched your clusters in> --cluster_id=<j-xxxxxxxxxxxx>`

###License

Distributed under the MIT license. See `LICENSE` for more information.
