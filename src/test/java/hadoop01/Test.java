package hadoop01;

import java.util.concurrent.*;

public class Test {
	public static void main(String[] args) {
		/*ExecutorService executorService = Executors.newCachedThreadPool(); // 线程池
		for(int i = 1; i < 101; i++){
			IsPrime isPrime = new IsPrime(i);
			Future<Boolean> future =  executorService.submit(isPrime);
			try {
				Boolean result = future.get(); //异步获取结果，阻塞线程
				if(result== true){
					System.out.println(i);
                }
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}*/

		System.out.println("192.168.1.0:1111".hashCode());

	}
}

class IsPrime implements Callable<Boolean>{
	private int num;

	public IsPrime(int num){
		this.num = num;
	}

	@Override
	public Boolean call() throws Exception {
		Boolean isPrime = true;
		for(int i = 2; i < num; i++){
			if(num % i == 0){
				isPrime = false;
				break;
			}
		}
		return isPrime;
	}
}

