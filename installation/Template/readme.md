##Template History

Installing Qlik-Sagemaker as Service 

https://medium.com/@Tankado95/how-to-run-a-python-code-as-a-service-using-systemctl-4f6ad1835bf2

  723  sudo systemctl enable qlik-sagemaker.service
  751  sudo systemctl daemon-reload
  752  sudo systemctl status qlik-sagemaker
  757  sudo systemctl edit qlik-sagemaker
  758  sudo systemctl edit qlik-sagemaker.service
  759  sudo systemctl edit --full  qlik-sagemaker.service

  change logger.config

  [loggers]
keys=root

[logger_root]
handlers=console,file
level=NOTSET

[formatters]
keys=simple,complex

[formatter_simple]
format=%(asctime)s - %(levelname)s - %(message)s

[formatter_complex]
format=%(asctime)s - %(levelname)s - %(module)s : %(lineno)d - %(message)s

[handlers]
keys=file,console

[handler_file]
class=handlers.TimedRotatingFileHandler
interval=midnight
backupCount=5
formatter=complex
level=INFO
args=('/home/centos/Qlik-Rapid-API-Gateway/logs/SSEPlugin.log',)

[handler_console]
class=StreamHandler
formatter=simple
level=NONE
args=(sys.stdout,)