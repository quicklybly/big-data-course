plugins {
    java
}

group = "com.quicklybly"
version = "1.0"

val sparkVersion = "4.3.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.spark:spark-sql_2.13:$sparkVersion")
}

val sparkJvmArgs = listOf(
    "-Xmx2g",
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-modules=jdk.incubator.vector",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "-Djdk.reflect.useDirectMethodHandle=false",
    "-Dio.netty.tryReflectionSetAccessible=true",
)

val dataDir = rootProject.layout.projectDirectory.dir("../data")

fun registerJob(name: String, mainClass: String, description: String, defaultArgs: List<String>) =
    tasks.register<JavaExec>(name) {
        group = "spark"
        this.description = description
        classpath = sourceSets.main.get().runtimeClasspath
        this.mainClass = mainClass
        jvmArgs(sparkJvmArgs)
        args(defaultArgs)
        javaLauncher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(17) }
    }

registerJob(
    "generateLog",
    "com.quicklybly.bigdata.spark.LogGenerator",
    "Generate a synthetic Apache access log in lab4/data",
    listOf(dataDir.file("access.log").asFile.path, "1000000"),
)

registerJob(
    "smoke",
    "com.quicklybly.bigdata.spark.SmokeCheck",
    "Start local Spark and read the log with both RDD and DataFrame APIs",
    listOf(dataDir.file("access.log").asFile.path),
)
