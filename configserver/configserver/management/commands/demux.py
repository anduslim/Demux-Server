from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    
    def handle(self, *args, **options):
        from configserver.mqttsub import MQTTDemuxClient
        myMqtt = MQTTDemuxClient()
        while(1):
            count = 1
        return 1
