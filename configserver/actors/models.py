from django.db import models

class Node(models.Model):
    ''' Node Related Information
    '''
    guid = models.IntegerField('Node ID', blank=False)

    conf_seq = models.ForeignKey('ConfigurationSequence', blank=False, null=False,
                        related_name='node')

    deployment = models.ForeignKey('Deployment', blank=False, null=False,
                        related_name='node')

    def __str__(self):
        return ', '.join([str(self.guid)])


class Deployment(models.Model):
    ''' Deployment Information
    '''

    name = models.CharField(max_length=128, blank=False)

    cluster = models.CharField(max_length=128, blank=True)

    location = models.CharField(max_length=128, blank=True)

    def __str__():
        return ', '.join([str(self.id), self.name])


class ConfigurationSequence(models.Model):
    ''' Configuration Sequence Information
    '''
    GATEWAY_NODE, SENSOR_NODE = range(2)
    NODE_CHOICES = (
            (GATEWAY_NODE, 'GatewayNode'),
            (SENSOR_NODE, 'SensorNode')
        )

    type_id = models.PositiveSmallIntegerField('Node Type', blank=False)

    ip_address = models.CharField(max_length=128, blank=True)

    gateway_ip = models.CharField(max_length=128, blank=True)    

    node_profile_type = models.PositiveSmallIntegerField(
                 default=SENSOR_NODE,
                 choices=NODE_CHOICES
            )

    def __str__(self):
        return ', '.join(['type ' + str(self.type_id), 
                        self.get_node_profile_type_display()])


class SensorMap(models.Model):
    ''' Sensor Map Information
    '''
    bit_position = models.PositiveSmallIntegerField()

    conf_seq = models.ForeignKey('ConfigurationSequence', blank=False, null=False,
                        related_name='sensor_map')

    sensor = models.ForeignKey('Sensor', blank=False, null=False,
                        related_name='sensor_map')

    def __str__(self):
        return ', '.join(['config_id-' + str(self.conf_seq_type_id), 'bit-' + 
                str(self.bit_position), self.sensor.modality])
   

class Sensor(models.Model):
    ''' Sensor Related Information
    '''
    
    name = models.CharField(max_length=128, blank=False)

    modality = models.CharField(max_length=128, blank=False)
            
    data_length = models.PositiveIntegerField('data length')

    data_api_name = models.CharField(max_length=128, blank=False)

    product_model = models.CharField(max_length=128)

    sensing_interval = models.PositiveIntegerField()

    def __str__(self):
        return ', '.join([self.modality, str(data_length)])


class Statistics(models.Model):
    ''' Stats Related Information
    '''
    
    name = models.CharField(max_length=128, blank=False)

    modality = models.CharField(max_length=128, blank=False)
            
    data_length = models.PositiveIntegerField('data length')

    data_api_name = models.CharField(max_length=128, blank=False)

    sensing_interval = models.PositiveIntegerField()

    def __str__(self):
        return ', '.join([self.modality, str(data_length)])
