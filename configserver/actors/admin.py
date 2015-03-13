from django.contrib import admin

from .models import(
                Node,
                Deployment,
                ConfigurationSequence,
                SensorMap,
                Sensor,
                Statistics
                )

admin.site.register(Node)
admin.site.register(Deployment)
admin.site.register(ConfigurationSequence)
admin.site.register(SensorMap)
admin.site.register(Sensor)
admin.site.register(Statistics)
