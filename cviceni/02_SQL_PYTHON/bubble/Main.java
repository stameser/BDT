package cz.cvut;
import com.sun.javaws.exceptions.InvalidArgumentException;

import java.time.LocalDateTime;


public class Main {

    public static void bubbleSort(int[] arr, boolean ascending) throws InvalidArgumentException {
        /*
        * Very long JAVA comment
        * */

        if (arr.length == 0){
            throw new InvalidArgumentException(new String[]{"Empty array!"});
        }

        boolean swapped = true;
        int j = 0;
        int tmp;
        // short comment
        while (swapped) {
            swapped = false;
            j++;
            for (int i = 0; i < arr.length - j; i++) {
                if (ascending) {
                    if (arr[i] > arr[i + 1]) {
                        tmp = arr[i];
                        arr[i] = arr[i + 1];
                        arr[i + 1] = tmp;
                        swapped = true;
                    }
                }
                else {
                    if (arr[i] < arr[i + 1]) {
                        tmp = arr[i];
                        arr[i] = arr[i + 1];
                        arr[i + 1] = tmp;
                        swapped = true;
                    }
                }
            }
        }
    }

    public static void bubbleSort(int[] arr) throws InvalidArgumentException {
        bubbleSort(arr, true);
    }

    public static void main(String[] args) throws InvalidArgumentException {
	    int[] a = new int[]{10, 4, 9, 4, 5};

        System.out.println(String.format("Hello, world, today is %s", LocalDateTime.now()));

	    bubbleSort(a, false);

        for (int i = 0; i < a.length; i++){
            System.out.print(a[i] + " ");
        }

        System.out.println("");
        System.out.println("Reverse");

        bubbleSort(a);

        for (int i = 0; i < a.length; i++){
            System.out.print(a[i] + " ");
        }
    }
}
