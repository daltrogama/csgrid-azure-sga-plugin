#!/bin/bash
JAR_WITH_DEPENDENCIES=fogbow-sga-0.0.1-SNAPSHOT-jar-with-dependencies.jar
ZIP_WITH_DEPENDENCIES=fogbow-sga-0.0.1-SNAPSHOT-jpf.zip

mvn clean install
cd target
mkdir sandbox
cp $JAR_WITH_DEPENDENCIES sandbox
cd sandbox
unzip $JAR_WITH_DEPENDENCIES
rm $JAR_WITH_DEPENDENCIES
cp ../../src/main/config/plugin.xml .
zip -r $ZIP_WITH_DEPENDENCIES *
cd ..
mv sandbox/$ZIP_WITH_DEPENDENCIES .
rm -rf sandbox
