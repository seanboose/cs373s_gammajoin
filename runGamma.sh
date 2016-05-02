#! /bin/bash

echo "Compiling TestFiles"

CLASSPATH="src:junit410.jar:RegTest.jar:out"
javac -d ./out -cp $CLASSPATH src/tests/*.java 

echo "---------- INDIVIDUAL MODULE TESTS ----------"
echo
echo "---------- READRELATION TEST ----------"
java -cp $CLASSPATH org.junit.runner.JUnitCore tests.ReadRelationTest
echo
echo

echo "---------- HJOIN TEST ----------"
java -cp $CLASSPATH org.junit.runner.JUnitCore tests.HJoinTest
echo
echo

echo "---------- BFILTER TEST ----------"
java -cp $CLASSPATH org.junit.runner.JUnitCore tests.BFilterTest
echo
echo

echo "---------- BLOOM TEST ----------"
java -cp $CLASSPATH org.junit.runner.JUnitCore tests.BloomTest
echo
echo

echo "---------- HSPLIT TEST ----------"
java -cp $CLASSPATH org.junit.runner.JUnitCore tests.HSplitTest
echo
echo

echo "---------- MERGE TEST ----------"
java -cp $CLASSPATH org.junit.runner.JUnitCore tests.MergeTest
echo
echo

echo "---------- SPLITM TEST ----------"
java -cp $CLASSPATH org.junit.runner.JUnitCore tests.SplitMTest
echo
echo

echo "---------- MERGEM TEST ----------"
java -cp $CLASSPATH org.junit.runner.JUnitCore tests.MergeMTest
echo
echo

echo "---------- PIPE N FILTER TESTS ----------"
echo
echo "---------- MRBFILTER TEST ----------"
java -cp $CLASSPATH org.junit.runner.JUnitCore tests.MRBFilterTest
echo
echo

echo "---------- MRHJOIN TEST ----------"
java -cp $CLASSPATH org.junit.runner.JUnitCore tests.MRHJoinTest
echo
echo

echo "---------- MRBLOOM TEST ----------"
java -cp $CLASSPATH org.junit.runner.JUnitCore tests.MRBloomTest
echo
echo

echo "---------- HJOINREFINE TEST ----------"
java -cp $CLASSPATH org.junit.runner.JUnitCore tests.HJoinRefineTest
echo
echo

echo "---------- GAMMA TEST ----------"
java -cp $CLASSPATH org.junit.runner.JUnitCore tests.GammaTest
echo
echo