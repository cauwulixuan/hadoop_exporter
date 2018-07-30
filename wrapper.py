# #!/usr/bin/python
# # -*- coding: utf-8 -*-

# from time import time, sleep

# def deco(func):
#     def wrapper():
#         start_time = time()
#         print "start time = %s" %start_time
#         func()
#         # sleep(1)
#         print "end time = %s, %s runs %s seconds." %(time(), func, time() - start_time)
#     return wrapper

# def deco2(func):
#     def wrapper(a, b):        
#         start_time = time()
#         print "start time = %s" %start_time
#         x = func(a, b)
#         # sleep(1)
#         print "end time = %s, %s runs %s seconds." %(time(), func, time() - start_time)
#         return x
#     return wrapper

# def deco3(args):
#     def deco(func):
#         def wrapper(a, b):
#             print "before %s called [%s]." % (func.__name__, args)
#             func(a, b)
#             # sleep(1)
#             print "after %s called [%s]." % (func.__name__, args)
#         return wrapper
#     return deco


# @deco
# def test():
#     print "This is a test message."

# @deco2
# def test2(a, b):
#     print "test2 mesg : {0} + {1} = {2}.".format(a, b, a + b)
#     return a + b

# @deco2
# @deco3("module")
# def test3(a, b):
#     print "test3 mesg : {0} * {1} = {2}.".format(a, b, a * b)
#     return a * b


# def main():
#     # test()
#     # test2(1,2)
#     # test2(2,3)
#     # test3(2,5)
#     test3(3,5)

# if __name__ == '__main__':
#     main()

def dec1(func):
    print "inner dec1"
    def one():
        print "before {} called".format(func.__name__)
        func()
        print "after {} called".format(func.__name__)
        return func
    print "still in dec1"
    return one

def dec2(func):
    print "inner dec2"
    def two():
        print "inner dec2.two"
        print "before {} called".format(func.__name__)
        func()
        print "after {} called".format(func.__name__)
        return func
    print "still in dec2"
    return two  

@dec1
@dec2
def test():
    print "inner test"

# 执行test()
test()
'''
多重装饰器进行装饰函数时，自下而上执行。
但是下层的装饰器的装饰体并不执行，而是直接把该函数返回给上层装饰器，当做上层装饰器的函数参数。
依次类推，直到最上层装饰器开始执行装饰体，再一层一层向下执行，直到内层装饰体执行完毕，被装饰的函数体也执行完毕，
依次返回给上层装饰器继续执行上层装饰器剩下的部分。
'''


print("===============================")
def wrapClass(cls):
    print "inside wrapClass"
    def inner(a):
        print 'class name: %s' %cls.__name__
        return cls(a)
    print "after inner"
    return inner


@wrapClass
class Foo():
    def __init__(self, a):
        self.a = a
        print "init Foo"

    def fun(self):
        print 'self.a = %s' %self.a


m = Foo('xiemanR')
m.fun()

print "========================="
def deco(func):
    print("before myfunc() called.")
    func()
    print("  after myfunc() called.")
    return func

# deco = deco(myfunc) 
@deco
def myfunc():
    print(" myfunc() called.")
 
myfunc()
myfunc()

print "=================="
def foo():
    class Foo(object):
        """docstring for ClassName"""
        def __init__(self):
            print "inner class"
        def bar(self):
            print "bar"
        print "end Foo class"

    a = Foo()
    a.bar()
    print "a = %s,type Foo: %s"%(a.__class__.__name__,type(a))
    print "end foo"


print "=============================="
def dec(func):
    a = 100
    def wrapper(*args, **kwargs):
        print "auth1"
        # func()
        return func(*args, **kwargs)
    return wrapper
    # print "auth2"
    # return func

@dec
def foo1(a,b,c):
    print a+b+c
    print "inner foo1"

b = foo1(1,2,3)
print b
foo1(2,3,4)


print "======================="
def foo2(x,y):
    return x+y

print map(foo2,[1,2,3,4], [1,1,1,1])
# print map(foo2, [1,2,3,4], ['a','b','c'])

from functools import reduce
print reduce(lambda x,y: x+y, [1,2,3,4,5], 100)

def str2int(s):
    seq = {'0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9}
    return reduce(lambda x,y: 10*x + y, map(lambda x: seq[x], s))
print str2int('12345')
print int('123456')


def normalize(name):
    return name.capitalize()
    # return map(lambda name: name[:1].upper()+name[1:].lower(), name)
L = ['adam', 'LISA', 'barT']
# print normalize(L)
print list(map(normalize, L))

def prod(L):
    return reduce(lambda x,y: x*y, L)
print '3 * 5 * 7 * 9 =%s'%prod([3, 5, 7, 9])


def str2float(s):
    seq = {'0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9}
    # 小数点之前
    if "." in s:
        s1 = s.split('.')[0]
        # 小数点之后
        s2 = s.split('.')[1]
        return reduce(lambda x,y: 10.0*x+y, map(lambda z: seq[z], s1+s2))/(10**(len(s2)))
    else:
        return reduce(lambda x,y: 10.0*x+y, map(lambda z: seq[z], s))
print str2float('12345678')


def is_palindrome(n):
    return str(n) == str(n)[::-1]

print filter(is_palindrome, range(1, 200))


dict1 = {"a1":"a11", "a2":"a22"}
dict2 = {"c1":"c11", "d2":"d22"}
# dict1.update(dict2)
# print dict1
d3 = {}
d3.update(dict1)
d3.update(dict2)
print d3