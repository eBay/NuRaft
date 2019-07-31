/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright 2017 Jung-Sang Ahn
See URL: https://github.com/greensky00/latency-collector
         (v0.1.2)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#pragma once

#include <stdint.h>

#include <atomic>
#include <mutex>

template<typename T>
class ashared_ptr {
public:
    ashared_ptr() : object(nullptr) {}
    ashared_ptr(T* src_ptr) : object( (src_ptr)
                                      ? new PtrWrapper<T>(src_ptr)
                                      : nullptr ) {}
    ashared_ptr(const ashared_ptr<T>& src) : object(nullptr) {
        operator=(src);
    }

    ~ashared_ptr() {
        reset();
    }

    void reset() {
        std::lock_guard<std::mutex> l(lock);
        PtrWrapper<T>* ptr = object.load(MO);
        // Unlink pointer first, destroy object next.
        object.store(nullptr, MO);
        releaseObject(ptr);
    }

    bool operator==(const ashared_ptr<T>& src) const {
        return object.load(MO) == src.object.load(MO);
    }

    bool operator==(const T* src) const {
        if (!object.load(MO)) {
            // If current `object` is NULL,
            return src == nullptr;
        }
        return object.load(MO)->ptr.load(MO) == src;
    }

    void operator=(const ashared_ptr<T>& src) {
        std::lock_guard<std::mutex> l(lock);

        ashared_ptr<T>& writable_src = const_cast<ashared_ptr<T>&>(src);
        PtrWrapper<T>* src_object = writable_src.shareCurObject();

        // Replace object.
        PtrWrapper<T>* old = object.load(MO);
        object.store(src_object, MO);

        // Release old object.
        releaseObject(old);
    }

    T* operator->() const { return object.load(MO)->ptr.load(MO); }
    T& operator*() const { return *object.load(MO)->ptr.load(MO); }
    T* get() const { return object.load(MO)->ptr.load(MO); }

    inline bool compare_exchange_strong(ashared_ptr<T>& expected,
                                        ashared_ptr<T> src,
                                        std::memory_order order)
    {
        (void)order;
        return compare_exchange(expected, src);
    }

    inline bool compare_exchange_weak(ashared_ptr<T>& expected,
                                      ashared_ptr<T> src,
                                      std::memory_order order)
    {
        (void)order;
        return compare_exchange(expected, src);
    }

    bool compare_exchange(ashared_ptr<T>& expected, ashared_ptr<T> src) {
        // Note: it is OK that `expected` becomes outdated.
        PtrWrapper<T>* expected_ptr = expected.object.load(MO);
        PtrWrapper<T>* val_ptr = src.shareCurObject();

        { // Lock for `object`
            std::lock_guard<std::mutex> l(lock);
            if (object.compare_exchange_weak(expected_ptr, val_ptr)) {
                // Succeeded.
                // Release old object.
                releaseObject(expected.object.load(MO));
                return true;
            }
        }
        // Failed.
        expected = *this;
        // Release the object from `src`.
        releaseObject(val_ptr);
        return false;
    }

private:
    template<typename T2>
    struct PtrWrapper {
        PtrWrapper() : ptr(nullptr), refCount(0) {}
        PtrWrapper(T2* src) : ptr(src), refCount(1) {}

        std::atomic<T2*> ptr;
        std::atomic<uint64_t> refCount;
    };

    // Atomically increase ref count and then return.
    PtrWrapper<T>* shareCurObject() {
        std::lock_guard<std::mutex> l(lock);
        if (!object.load(MO)) return nullptr;

        // Now no one can change `object`.
        // By increasing its ref count, `object` will be safe
        // until the new holder (i.e., caller) is destructed.
        object.load(MO)->refCount.fetch_add(1, MO);
        return object.load(MO);
    }

    // Decrease ref count and delete if no one refers to it.
    void releaseObject(PtrWrapper<T>* target) {
        if (!target) return;
        if (target->refCount.fetch_sub(1, MO) == 1) {
            // Last shared pointer, delete it.
            delete target->ptr.load(MO);
            delete target;
        }
    }

    const static std::memory_order MO = std::memory_order_relaxed;

    std::atomic<PtrWrapper<T>*> object;
    std::mutex lock;
};
