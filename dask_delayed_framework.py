from dask import delayed

class MyFramework:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def compute(self):
        # This is a simple function that uses Dask under the hood
        result = delayed(self.x + self.y)
        return result.compute()

# Users will interact with your class, not Dask directly
framework = MyFramework(1, 2)
result = framework.compute()