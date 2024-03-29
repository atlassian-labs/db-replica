buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        classpath("info.solidsoft.gradle.pitest:gradle-pitest-plugin:1.4.7")
    }
}

plugins {
    `java-library`
    id("com.atlassian.performance.tools.gradle-release").version("0.7.3")
    id("info.solidsoft.pitest").version("1.6.0")
}

configure<info.solidsoft.gradle.pitest.PitestPluginExtension> {
    junit5PluginVersion.set("0.12")
    avoidCallsTo.set(setOf("kotlin.jvm.internal", "kotlinx.coroutines"))
    mutators.set(setOf("STRONGER"))
    targetClasses.set(setOf("com.atlassian.db.replica.*"))
    targetTests.set(setOf("*Test"))
    threads.set(Runtime.getRuntime().availableProcessors())
    outputFormats.set(setOf("XML", "HTML"))
}

tasks.wrapper {
    gradleVersion = "6.7"
    distributionType = Wrapper.DistributionType.BIN
}

dependencies {
    testImplementation("org.postgresql:postgresql:42.2.18")
    testImplementation(platform("org.junit:junit-bom:5.7.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.19.0")
    testImplementation("org.mockito:mockito-core:3.11.0")
    testImplementation("org.mockito:mockito-junit-jupiter:3.11.0")
    testImplementation("org.threeten:threeten-extra:1.5.0")
    testImplementation("com.github.docker-java:docker-java-core:3.2.6")
    testImplementation("com.github.docker-java:docker-java-transport-httpclient5:3.2.6")
    testImplementation("com.h2database:h2:1.4.200")
}

configurations.all {
    resolutionStrategy {
        activateDependencyLocking()
        failOnVersionConflict()
        eachDependency {
            resolveBogusConflicts()
        }
    }
}

/**
 * Third-party libraries often declare specific versions of their dependencies.
 * But they're de facto compatible with a wider range of versions.
 * This leads [ResolutionStrategy.failOnVersionConflict] to detect bogus conflicts.
 * Preferably, the lib authors would relax their version requirement.
 * Otherwise we have to find a compatible version and force it here.
 */
fun DependencyResolveDetails.resolveBogusConflicts() {
    when (requested.module.toString()) {
        "org.slf4j:slf4j-api" -> useVersion("1.7.30")
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks.compileJava {
    options.compilerArgs.add("-Xlint:deprecation")
    options.compilerArgs.add("-Xlint:unchecked")
}

tasks.withType<Test> {
    reports {
        junitXml.isEnabled = true
    }
}

tasks.test {
    useJUnitPlatform()
    filter {
        exclude("**/*IT.class")
    }
}

val testIntegration = task<Test>("testIntegration") {
    useJUnitPlatform()
    filter {
        include("**/*IT.class")
    }
    setForkEvery(1)
    maxParallelForks = 1
}

tasks["check"].dependsOn(testIntegration)

group = "com.atlassian.db.replica"

gradleRelease {
    atlassianPrivateMode = true
}
