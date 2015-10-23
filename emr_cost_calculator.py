#!/usr/bin/env python
"""EMR cost calculator

Usage:
    emr_cost_calculator.py total --region=<reg> \
--created_after=<ca> --created_before=<cb> \
[--aws_access_key_id=<ai> --aws_secret_access_key=<ak>]
    emr_cost_calculator.py cluster --region=<reg> --cluster_id=<ci> \
[--aws_access_key_id=<ai> --aws_secret_access_key=<ak>]
    emr_cost_calculator.py -h | --help


Options:
    -h --help                     Show this screen
    total                         Calculate the total EMR cost \
for a period of time
    cluster                       Calculate the cost of single \
cluster given the cluster id
    --region=<reg>                The aws region that the \
cluster was launched on
    --aws_access_key_id=<ai>      Self-explanatory
    --aws_secret_access_key=<ci>  Self-explanatory
    --created_after=<ca>          The calculator will compute \
the cost for all the cluster created after the created_after day
    --created_before=<cb>         The calculator will compute \
the cost for all the cluster created before the created_before day
    --cluster_id=<ci>             The id of the cluster you want to \
calculate the cost for
"""

from docopt import docopt
import boto.emr
from retrying import retry
import sys
import time
import math
import yaml
import datetime


config = yaml.load(open('config.yml', 'r'))
prices = config['prices']


def validate_date(date_text):
    try:
        return datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
       raise ValueError('Incorrect data format, should be YYYY-MM-DD')


def retry_if_EmrResponseError(exception):
    """
    Use this function in order to back off only
    on EmrResponse errors and not in other exceptions
    """
    return isinstance(exception, boto.exception.EmrResponseError)


class Ec2Instance:

    def __init__(self, creation_ts, termination_ts, instance_price):
        self.lifetime = self._get_lifetime(creation_ts, termination_ts)
        self.cost = self.lifetime * instance_price

    @staticmethod
    def _parse_dates(creation_ts, termination_ts):
        """
        :param creation_ts: the creation time string
        :param termination_ts: the termination time string
        :return: the lifetime of a single instance in hours
        """
        date_format = '%Y-%m-%dT%H:%M:%S.%fZ'
        creation_ts = \
            time.mktime(time.strptime(creation_ts, date_format))
        termination_ts = \
            time.mktime(time.strptime(termination_ts, date_format))
        return creation_ts, termination_ts

    def _get_lifetime(self, creation_ts, termination_ts):
        """
        :param creation_ts: the creation time string
        :param termination_ts: the termination time string
        :return: the lifetime of a single instance in hours
        """
        (creation_ts, termination_ts) = \
            Ec2Instance._parse_dates(creation_ts, termination_ts)
        return math.ceil((termination_ts - creation_ts) / 3600)


class InstanceGroup:

    def __init__(self, group_id, instance_type, group_type):
        self.group_id = group_id
        self.instance_type = instance_type
        self.group_type = group_type
        self.price = 0


class EmrCostCalculator:

    def __init__(self, region, aws_access_key_id=None, aws_secret_access_key=None):
        try:
            print >> sys.stderr, \
                '[INFO] Retrieving cost in region %s' \
                % (region)
            self.conn = \
                boto.emr.connect_to_region(
                    region,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)
            self.spot_used = False
        except:
            print >> sys.stderr, \
                '[ERROR] Could not establish connection with EMR api'

    def get_total_cost_by_dates(self, created_after, created_before):
        total_cost = 0
        for cluster_id in \
                self._get_cluster_list(created_after, created_before):
            cost_dict = self.get_cluster_cost(cluster_id)
            total_cost += cost_dict['TOTAL']
        return total_cost

    @retry(wait_exponential_multiplier=1000,
           wait_exponential_max=7000,
           retry_on_exception=retry_if_EmrResponseError)
    def get_cluster_cost(self, cluster_id):
        """
        Joins the information from the instance groups and the instances
        in order to calculate the price of the whole cluster

        It is important that we use a backoff policy in this case since Amazon
        throttles the number of API requests.
        :return: A dictionary with the total cost of the cluster and the
                individual cost of each instance group (Master, Core, Task)
        """
        instance_groups = self._get_instance_groups(cluster_id)
        cost_dict = {}
        for instance_group in instance_groups:
            for instance in self._get_instances(instance_group, cluster_id):
                cost_dict.setdefault(instance_group.group_type, 0)
                cost_dict[instance_group.group_type] += instance.cost
                cost_dict.setdefault('TOTAL', 0)
                cost_dict['TOTAL'] += instance.cost

        return EmrCostCalculator._sanitise_floats(cost_dict)

    @staticmethod
    def _sanitise_floats(aDict):
        """
        Round the values to 3 decimals.
        #Did it this way to avoid
        https://docs.python.org/2/tutorial/floatingpoint.html#representation-error
        """
        for key in aDict:
            aDict[key] = round(aDict[key], 3)
        return aDict

    def _get_cluster_list(self, created_after, created_before):
        """
        :return: An iterator of cluster ids for the specified dates
        """
        marker = None
        while True:
            cluster_list = \
                self.conn.list_clusters(created_after,
                                        created_before,
                                        marker=marker)
            for cluster in cluster_list.clusters:
                yield cluster.id
            try:
                marker = cluster_list.marker
            except AttributeError:
                break

    def _get_instance_groups(self, cluster_id):
        """
        Invokes the EMR api and gets a list of the cluster's instance groups.
        :return: List of our custom InstanceGroup objects
        """
        groups = self.conn.list_instance_groups(cluster_id).instancegroups
        instance_groups = []
        for group in groups:
            inst_group = InstanceGroup(group.id,
                                       group.instancetype,
                                       group.instancegrouptype)
            # If is is a spot instance get the bidprice
            if group.market == 'SPOT':
                inst_group.price = float(group.bidprice)
            else:
                inst_group.price = prices[group.instancetype]
            instance_groups.append(inst_group)
        return instance_groups

    def _get_instances(self, instance_group, cluster_id):
        """
        Invokes the EMR api to retrieve a list of all the instances
        that were used in the cluster.
        This list is then joind to the InstanceGroup list
        on the instance group id
        :return: An iterator of our custom Ec2Instance objects.
        """
        instance_list = \
            self.conn.list_instances(cluster_id, instance_group.group_id)\
            .instances
        for instance_info in instance_list:
            try:
                end_date_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                if hasattr(instance_info.status.timeline, 'enddatetime'):
                    end_date_time = instance_info.status.timeline.enddatetime

                inst = Ec2Instance(
                            instance_info.status.timeline.creationdatetime,
                            end_date_time,
                            instance_group.price)
                yield inst
            except AttributeError as e:
                print >> sys.stderr, \
                    '[WARN] Error when computing instance cost. Cluster: %s'\
                    % cluster_id
                print >> sys.stderr, e


if __name__ == '__main__':
    args = docopt(__doc__)
    if args.get('total'):
       created_after = validate_date(args.get('--created_after'))
       created_before = validate_date(args.get('--created_before'))
       calc = EmrCostCalculator(args.get('--region'),
                                args.get('--aws_access_key_id'),
                                args.get('--aws_secret_access_key'))
       print calc.get_total_cost_by_dates(created_after, created_before)
    elif args.get('cluster'):
       print args.get('--region')
       calc = EmrCostCalculator(args.get('--region'),
                                args.get('--aws_access_key_id'),
                                args.get('--aws_secret_access_key'))
       print calc.get_cluster_cost(args.get('--cluster_id'))
    else:
       print >> sys.stderr, \
       '[ERROR] Invalid operation, please check usage again'
