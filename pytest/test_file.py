# Databricks notebook source
def add(a, b):
    return a + b

def test_add():
    assert add(2, 3) == 5
    print("working")

test_add()
