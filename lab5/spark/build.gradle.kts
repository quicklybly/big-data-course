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
    "-Xmx4g",
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

val input = rootProject.layout.projectDirectory.file("../../lab4/data/access.log").asFile.path
val outDir = rootProject.layout.projectDirectory.dir("../data/out")
val runs = (findProperty("runs") as String?) ?: "5"
val modes = (findProperty("modes") as String?) ?: "all"

fun registerJob(name: String, mainClass: String, desc: String, defaultArgs: List<String>) =
    tasks.register<JavaExec>(name) {
        group = "spark"
        description = desc
        classpath = sourceSets.main.get().runtimeClasspath
        this.mainClass = mainClass
        jvmArgs(sparkJvmArgs)
        args(defaultArgs)
        javaLauncher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(17) }
    }

registerJob(
    "dfBench",
    "com.quicklybly.bigdata.spark.DataFrameExperiment",
    "Benchmark variants",
    listOf(input, outDir.asFile.path, runs, modes),
)
