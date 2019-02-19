import boto3
import os
import re
from time import sleep, time
from kubernetes import client, config
from lib.utils import logger, queue_url_region, enforce_env_vars


class DeploymentNotFoundError(Exception):
    def __init__(self, deployment_name):
        super(DeploymentNotFoundError, self).__init__('Deployment "%s" does not exist' % deployment_name)


class SQSPoller:
    REQUIRED_ENV_VARS    = ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY')
    options              = None
    sqs_client           = None
    apps_v1              = None
    last_scale_up_time   = 0
    last_scale_down_time = 0
    logger               = logger('sqs-queue-autoscaler')

    def __init__(self, options):
        self.options = options

        self.logger.debug(self.options)

        enforce_env_vars(self.REQUIRED_ENV_VARS)

        self.sqs_client = boto3.client('sqs',
            region_name           = queue_url_region(self.options.sqs_queue_url),
            aws_access_key_id     = os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY'))

        config.load_incluster_config()
        self.apps_v1 = client.AppsV1Api()

    def run(self):
        '''
        Main entry point
        '''
        self.logger.debug('Starting poll for {} every {}s'.format(self.options.sqs_queue_url, self.options.poll_period))

        while True:
            self.poll()
            sleep(self.options.poll_period)

    def poll(self):
        try:
            messages = self.get_number_of_messages()
            self.logger.debug('Messages in the queue: %s' % messages)

            deployment = self.get_deployment()
            pods       = self.get_pods_delta(deployment, messages)
            self.logger.debug('Pods delta: %s' % pods)
            if pods != 0:
                self.safe_update_deployment(deployment, pods)

        except Exception as e:
            self.logger.error("%s: %s" % (e.__class__.__name__, str(e)))

    def get_number_of_messages(self):
        '''
        Returns a number of messages in the queue

        Returns
        -------
        int
        '''
        response = self.sqs_client.get_queue_attributes(
            QueueUrl       = self.options.sqs_queue_url,
            AttributeNames = ['ApproximateNumberOfMessages']
        )
        return int(response['Attributes']['ApproximateNumberOfMessages'])

    def get_pods_delta(self, deployment, messages):
        '''
        Returns a number of pods that need to be added or removed from a given
        deployment.

        Params
        ------
        deployment: kubernetes.client.models.v1_deployment.V1Deployment
        messages: int

        Returns
        -------
        int
            Positive when pods have to be added, negative when they have to be
            removed and 0 if there are no changes.
        '''
        pods = deployment.spec.replicas

        # Do we have at least min required pods running?
        missing = self.options.min_pods - pods
        if missing > 0:
            return missing

        # Do we have too many pods running?
        excessive = pods - self.options.max_pods
        if excessive > 0:
            return -excessive

        # So we are within allowed min/max pod boundaries.
        # Do we need to scale it up by a pod?
        if messages >= self.options.scale_up_messages \
            and pods < self.options.max_pods:
                return 1

        # Do we need to scale it down by a pod?
        if messages <= self.options.scale_down_messages \
            and pods > self.options.min_pods:
                return -1

        return 0

    def get_scale_up_delay(self):
        '''
        Returns a number of we have to wait before next scale up can be performed.
        If it is allowed now, then the method returns 0.

        Returns
        -------
        int
            Positive number or zero
        '''
        delay = self.last_scale_up_time + self.options.scale_up_cool_down - time()
        if delay < 0:
            delay = 0
        return delay

    def get_scale_down_delay(self):
        '''
        Returns a number of we have to wait before next scale down can be performed.
        If it is allowed now, then the method returns 0.

        Returns
        -------
        int
            Positive number or zero
        '''
        delay = self.last_scale_down_time + self.options.scale_down_cool_down - time()
        if delay < 0:
            delay = 0
        return delay

    def get_deployment(self):
        '''
        Gets deployment data

        Returns
        -------
        kubernetes.client.models.v1_deployment.V1Deployment

        Raises
        ------
        DeploymentNotFoundError
            when deployment does not exist

        '''
        label_selector = 'app={}'.format(self.options.kubernetes_deployment)
        deployments = self.apps_v1.list_namespaced_deployment(self.options.kubernetes_namespace, label_selector=label_selector)

        if len(deployments.items) == 0:
            raise DeploymentNotFoundError(self.options.kubernetes_deployment)

        return deployments.items[0]

    def safe_update_deployment(self, deployment, pods):
        '''
        Adds/removes pod(s) to/from a given deployment

        Params
        ------
        deployment: kubernetes.client.models.v1_deployment.V1Deployment
        pods: int
        '''
        if pods > 0:
            delay = self.get_scale_up_delay()
            if delay == 0:
                self.last_scale_up_time = time()
                return self.update_deployment(deployment, pods)
            self.logger.info('Increase by %s pods is requested but blocked until %.2f sec from now' % (pods, round(delay, 2)))

        elif pods < 0:
            delay = self.get_scale_down_delay()
            if delay == 0:
                self.last_scale_down_time = time()
                return self.update_deployment(deployment, pods)
            self.logger.info('Decrease by %s pods is requested but blocked until %.2f sec from now' % (-pods, round(delay, 2)))

    def update_deployment(self, deployment, pods):
        '''
        Adds/removes pod(s) to/from a given deployment

        Params
        ------
        deployment: kubernetes.client.models.v1_deployment.V1Deployment
        pods: int
        '''
        self.logger.info('Updating number of pods by %s' % pods)

        deployment.spec.replicas += pods
        if deployment.spec.replicas < 0:
            deployment.spec.replicas = 0

        # Update the deployment
        api_response = self.apps_v1.patch_namespaced_deployment(
            namespace = self.options.kubernetes_namespace,
            name      = self.options.kubernetes_deployment,
            body      = deployment)

        self.logger.debug('Deployment updated. status="%s"' % str(api_response.status))
