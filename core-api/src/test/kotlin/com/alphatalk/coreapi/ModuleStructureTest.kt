package com.alphatalk.coreapi

import org.junit.jupiter.api.Test
import org.springframework.modulith.core.ApplicationModules

class ModuleStructureTest {
    private val modules = ApplicationModules.of(CoreApiApplication::class.java)

    @Test
    fun `모듈 간 의존 규칙을 지킨다`() {
        modules.verify()
    }
}
