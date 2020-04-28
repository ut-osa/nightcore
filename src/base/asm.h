#pragma once

namespace faas {

inline void asm_volatile_memory() {
    asm volatile("" : : : "memory");
}

inline void asm_volatile_pause() {
    asm volatile("pause");
}

}  // namespace faas
