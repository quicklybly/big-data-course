plugins {
    java
}

group = "com.quicklybly"
version = "1.0"

val hadoopVersion = "3.5.0"
val coreNlpVersion = "4.6.0"

val taggerModel = configurations.create("taggerModel") {
    isTransitive = false
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.hadoop:hadoop-client-api:$hadoopVersion")

    // only MaxentTagger and Morphology are used, they need none of the CoreNLP dependencies
    implementation("edu.stanford.nlp:stanford-corenlp:$coreNlpVersion") {
        isTransitive = false
    }
    taggerModel("edu.stanford.nlp:stanford-corenlp:$coreNlpVersion:models")

    testImplementation("org.apache.hadoop:hadoop-client-api:$hadoopVersion")
    testRuntimeOnly("org.apache.hadoop:hadoop-client-runtime:$hadoopVersion")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.27.7")
    testImplementation("org.mockito:mockito-core:5.24.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val extractTaggerModel = tasks.register<Copy>("extractTaggerModel") {
    from({ zipTree(taggerModel.singleFile) }) {
        include("edu/stanford/nlp/models/pos-tagger/english-left3words-distsim.tagger*")
    }
    into(layout.buildDirectory.dir("generated/tagger-model"))
}

sourceSets.main {
    resources.srcDir(extractTaggerModel)
}

tasks.test {
    useJUnitPlatform()
    // Mockito's inline mock maker extends the boot classpath, which conflicts with class data sharing
    jvmArgs("-Xshare:off")
}

tasks.jar {
    archiveFileName = "topk-jobs.jar"
    from({ configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) } })
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    manifest {
        attributes("Main-Class" to "com.quicklybly.bigdata.topk.TopKDriver")
    }
}
