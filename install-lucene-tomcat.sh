#!/bin/bash
mvn -Dmaven.test.skip=true clean package install -P lucene,tomcat assembly:assembly -U