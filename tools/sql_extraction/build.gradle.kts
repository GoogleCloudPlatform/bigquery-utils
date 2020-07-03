plugins {
    application
    antlr
    kotlin("jvm") version "1.3.72"
    id("de.undercouch.download") version "4.0.4"
}

group = "com.google.cloud.sqlecosystem"
version = "1.0"

// clean task = clean
// build task = build
// build and run task = run
// test task = test
application {
    mainClassName = "com.google.cloud.sqlecosystem.sqlextraction.CliKt"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("io.github.microutils:kotlin-logging:1.7.9")
    implementation("org.slf4j:slf4j-simple:1.7.29")
    antlr("org.antlr:antlr4:4.7.2")
    implementation("org.antlr:antlr4-runtime:4.7.2")
    implementation("com.github.ajalt:clikt:2.7.1")
    implementation("com.google.code.gson:gson:2.8.5")

    testImplementation(kotlin("test"))
    testImplementation(kotlin("test-junit"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
    testImplementation("com.google.jimfs:jimfs:1.1")
    testImplementation("io.mockk:mockk:1.10.0")
}

task<de.undercouch.gradle.tasks.download.Download>("downloadGrammars") {
    doFirst {
        mkdir("gen/main/antlr")
        mkdir("src/main/antlr")
    }
    src(listOf("https://raw.githubusercontent.com/antlr/grammars-v4/master/java/java9/Java9.g4"))
    dest("gen/main/antlr/")
    overwrite(false) // no need to download a copy every time
    println(System.getProperty("user.dir"))
}

tasks {
    generateGrammarSource {
        dependsOn("downloadGrammars")
        arguments = arguments + listOf(
            "-package", "com.google.cloud.sqlecosystem.sqlextraction.antlr",
            "-visitor",
            "-no-listener"
        )
        include("*.g4")
        source("gen/main/antlr")
        outputDirectory = File("gen/main/antlr")
        doLast {
            mkdir("gen/main/java/com/google/cloud/sqlecosystem/sqlextraction/antlr")
            copy {
                from("gen/main/antlr/") {
                    include("*.java")
                }
                into("gen/main/java/com/google/cloud/sqlecosystem/sqlextraction/antlr/")
            }
        }
    }

    compileKotlin {
        dependsOn(generateGrammarSource)
        kotlinOptions.jvmTarget = "1.8"
    }

    compileTestKotlin {
        dependsOn(generateTestGrammarSource)
        kotlinOptions.jvmTarget = "1.8"
    }

    clean {
        doFirst {
            delete("gen")
        }
    }
}

sourceSets {
    main {
        java {
            srcDirs("gen/main/java")
        }
    }
}
