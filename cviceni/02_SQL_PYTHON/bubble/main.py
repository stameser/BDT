import datetime

def bubble_sort(arr, ascending=True):
    """
    Very long comment
    """

    if len(arr) == 0:
        raise ValueError("Empty array!")

    swapped = True
    j = 0
    tmp = 0
    # small comment
    while swapped:
        swapped = False
        j+= 1
        for i in xrange(len(arr) - j):
            if ascending:
                if arr[i] > arr[i + 1]:
                    tmp = arr[i]
                    arr[i] = arr[i + 1]
                    arr[i + 1] = tmp
                    swapped = True
            else:
                if arr[i] < arr[i + 1]:
                    tmp = arr[i]
                    arr[i] = arr[i + 1]
                    arr[i + 1] = tmp
                    swapped = True

if __name__ == '__main__':

    print 'Hello, world, today is {0}'.format(datetime.datetime.now())

    a = [10, 4, 9, 4, 5]

    bubble_sort(a)
    print a

    print 'reverse'

    bubble_sort(a, ascending=False)
    print a
