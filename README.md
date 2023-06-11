# pyspark
`What is PySpark?`
PySpark is an interface for Apache Spark in Python. With PySpark, you can write Python and SQL-like commands to manipulate and analyze data in a distributed processing environment. 

`Why PySpark?`
The reason companies choose to use a framework like PySpark is because of how quickly it can process big data. It is faster than libraries like Pandas and Dask, and can handle larger amounts of data than these frameworks. If you had over petabytes of data to process, for instance, Pandas and Dask would fail but PySpark would be able to handle it easily.

`Pyspark = Python + Apache Spark`
Apache Spark is a new and open-source framework used in the big data industry for real-time processing and batch processing. It supports different languages, like Python, Scala, Java, and R.
Apache Spark is initially written in a Java Virtual Machine(JVM) language called Scala, whereas Pyspark is like a Python API which contains a library called Py4J. This allows dynamic interaction with JVM objects.

### Installation of PySpark (All operating systems)
> https://www.datacamp.com/tutorial/installation-of-pyspark
> https://phoenixnap.com/kb/set-environment-variable-mac

```
### Installing packages using pip and virtual environments
> https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/
```

```commandline
# echo "# pyspark_test" >> README.md
# git init
# git add README.md
# git commit -m "first commit"
# git branch -M main
# git remote add origin git@github.com:goldin2008/pyspark_test.git
# git push -u origin main
```

```commandline
pip install --upgrade pip
pip --version
python3 -m pip install --user virtualenv
rm -rf pyenv
python3 -m venv pysparkenv
source pysparkenv/bin/activate
```

### Reference
> https://realpython.com/pyspark-intro/

> https://www.datacamp.com/tutorial/pyspark-tutorial-getting-started-with-pyspark

> https://www.guru99.com/pyspark-tutorial.html

> https://towardsdatascience.com/beginners-guide-to-pyspark-bbe3b553b79f

> https://sparkbyexamples.com/pyspark-tutorial/

> https://mungingdata.com/pyspark/testing-pytest-chispa/