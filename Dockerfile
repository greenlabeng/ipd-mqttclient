FROM python:2-onbuild

RUN cp ipd.ini.sample ipd.ini

ENTRYPOINT [ "./docker-entrypoint.sh" ]

CMD [ "python",  "./MqttIpdPublish.py" ]
