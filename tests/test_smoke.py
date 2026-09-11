def test_local_spark_session(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    assert df.count() == 2
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"
