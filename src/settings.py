config_file_path='src/configs/inputconfig.yaml'
appName='CarCrashAnalysis'
logLevel="ERROR"
sparkConfig={
                "sparkConfigDriverOptions" : "spark.driver.extraJavaOptions",
                "sparkConfigAddOpens" : "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
            }