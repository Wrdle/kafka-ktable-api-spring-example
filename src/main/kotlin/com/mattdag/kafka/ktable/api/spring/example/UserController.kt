package com.mattdag.kafka.ktable.api.spring.example

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("user")
class UserController(private val kTableTopology: KTableTopology) {
//    @GetMapping("/{id}")
//    fun getUser(@PathVariable id: String): String {
//        return kTableTopology.getById(id)
//    }
}
