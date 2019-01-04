buildscript {
    repositories {
        mavenCentral()
        gradlePluginPortal()
        jcenter()
    }
}
plugins {
    scala
    id("org.dvaske.gradle.git-build-info") version "0.2"
    id("com.github.johnrengelman.shadow") version "4.0.3"
    id("com.github.maiflai.scalatest") version "0.23"
}
//allprojects {
repositories {
    mavenCentral()
}
//    apply<ScalaPlugin>()
//    apply(plugin = "com.github.johnrengelman.shadow")
//    apply(plugin = "com.github.maiflai.scalatest")
group = "rdb4s"
version = "0.1"
tasks.withType<Jar> {
    baseName = "wafna"
    appendix = project.name
    classifier = "ALPHA"
    manifest {
        attributes(
                Pair("Implementation-Title", project.name),
                Pair("Implementation-Version", project.version)/*,
            Pair("SHA", gitHead),
            Pair("DESC", gitDescribeInfo)*/)
    }
}
tasks.withType(ScalaCompile::class.java).configureEach {
    dependencies {
        compile("org.scala-lang", "scala-library", "2.12.8")
        testCompile("org.scalatest", "scalatest_2.12", "3.0.5")
        testCompile("org.pegdown", "pegdown", "1.4.2")
    }
    scalaCompileOptions.isDeprecation = true
    scalaCompileOptions.isUnchecked = true
    scalaCompileOptions.additionalParameters = listOf(
            "-unchecked",
            "-deprecation",
            "-feature", "-encoding", "utf8")
}
//}
// Makes slightly less noise in the output.
dependencies {
    testCompile("com.codahale.metrics", "metrics-core", "3.0.2")
    testCompile("org.hsqldb", "hsqldb", "2.4.0")
}
