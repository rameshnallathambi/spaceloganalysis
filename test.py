import pyspark
import pyspark.sql

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("test").setMaster("local")
spark_context = SparkContext(conf=conf)


val = ['alyssa.prodigy.com','www-d1.proxy.aol.com', 'piweba4y.prodigy.com','piweba2y.prodigy.com', 'www-b3.proxy.aol.com','columbia.acc.brad.ac.uk',
 'spectrum.xerox.com', 'beglinger.dial-up.bdt.com','www-d3.proxy.aol.com', 'freenet.edmonton.ab.ca',
 'dd08-021.compuserve.com', 'netcom3.netcom.com', 'www-b5.proxy.aol.com','disarray.demon.co.uk', 'ottgate2.bnr.ca', 'www-a2.proxy.aol.com',
 'pm206-52.smartlink.net', 'vagrant.vf.mmc.com', 'www-a1.proxy.aol.com','alpha2.csd.uwm.edu', 'piweba1y.prodigy.com',
 'srv1.freenet.calgary.ab.ca', 'hitiij.hitachi.co.jp','ccn.cs.dal.ca', 'wwwproxy.info.au', 'www-d2.proxy.aol.com', 'server.elysian.net',
 'hella.stm.it', 'piweba3y.prodigy.com','ntigate.nt.com','www-b2.proxy.aol.com', 'palona1.cns.hp.com','www-d4.proxy.aol.com', 'bettong.client.uq.oz.au','koala.melbpc.org.au', 'magicall.dacom.co.kr',
 'reggae.iinet.net.au']


def test_spark_context_fixture_test1():
    space = spark_context.textFile("same_hosts")


    assert space.count() == 37


def test_spark_context_fixture_test4():
    space = spark_context.textFile("same_hosts")


    assert space.count() == 37


def test_spark_context_fixture_test2():
    space = spark_context.textFile("same_hosts")


    assert type(space) == pyspark.rdd.RDD

def test_spark_context_fixture_test3():
    space = spark_context.textFile("same_hosts")


    assert set(val).issubset(set(space.collect()))







