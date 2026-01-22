plugins {
    java
    antlr
    application
}

group = "com.minimysql"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

dependencies {
    // ANTLR4 for SQL parsing
    antlr("org.antlr:antlr4:4.13.0")
    implementation("org.antlr:antlr4-runtime:4.13.0")

    // Logging: SLF4J + Logback
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("ch.qos.logback:logback-classic:1.4.11")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("com.minimysql.MiniMySQL")
}

tasks.test {
    useJUnitPlatform()
}

tasks.compileJava {
    options.encoding = "UTF-8"
}

// Generate ANTLR sources
tasks.generateGrammarSource {
    arguments = arguments + listOf("-visitor", "-no-listener", "-package", "com.minimysql.parser")
}
