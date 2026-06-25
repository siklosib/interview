# Task Description

## Setup
- Use Python 3.12+
- Create a virtual environment
- Install required libraries (pandas, pyarrow, etc...)

## Guidelines
- Write clear, maintainable, and well-structured code.
- Organize the data in a way that supports future extensions.
- Think carefully about how to model and process the data.
- Provide basic documentation and instructions on how to run your solution.
- Explain your thought process, what you are doing and why.

## Testing
- Include unit tests for core transformations.

## Problem 1
You will work with a small subset of the NYC Taxi dataset (already in `data/source/`).  
The goal is to compute the **most frequent pickup and drop-off zones**.

## Problem 2
Some drivers cheat by taking a much longer route than necessary between two points
(known as "long-hauling"). Using the same dataset, the goal is to **flag trips whose
distance is unusually long for the route they travelled**.
