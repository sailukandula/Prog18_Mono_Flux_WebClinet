package com.example.Prog18_Mono_Flex_WebClinet;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.List;

@SpringBootApplication
public class Prog18MonoFluxWebClinetApplication {

	public static void main(String[] args) throws InterruptedException {
		//SpringApplication.run(Prog18MonoFluxWebClinetApplication.class, args);

		/*The subscribe method expects a Consumer<T> (a functional interface from Oracle Java).
			Equivalent code without lambda:

			mono.subscribe(new Consumer<String>() {
    		@Override
   			public void accept(String str) {
        	System.out.println(str);
   			}
		});

		Your lambda is just a shorter way to write the same thing:
         str -> System.out.println(str)*/

		Mono<String> mono = Mono.just("Innovative");
		mono.subscribe(str -> System.out.println(str));

		List<String> strings = List.of("c","c++","java",".net");

		Mono<List<String>> listMono = Mono.just(strings);
		listMono.subscribe(lst ->System.out.println(lst));

		Flux<String> strFlux = Flux.just("c","c++","java",".net");
		strFlux.subscribe(str ->System.out.println(str));

		Flux<Integer> intFlux = Flux.just(10,20,30,40);
		Flux<Integer> mappedFlux = intFlux.map(value -> value *2);

		mappedFlux.subscribe(value ->System.out.println(value));

		//List using Flux
		Flux<String> listToFlux = Flux.fromIterable(strings).map(str ->str.toUpperCase());
		listToFlux.subscribe(str  -> System.out.println(str));

       //Alternate way to display list
		Flux.fromIterable(strings)
			.map(str -> str.toUpperCase())
			.delayElements(Duration.ofSeconds(1))
			.subscribe(System.out::println);

		//Stop main method stop
		Thread.sleep(5000);

		Sinks.Many<String> sinks = Sinks.many().multicast().onBackpressureBuffer();
		Flux<String> stringFluxSinks = sinks.asFlux();
		stringFluxSinks.subscribe(str -> System.out.println(str));
		stringFluxSinks.subscribe(str -> System.out.println(str));

		String iss = "Innovative";
		for (int i=0;i<iss.length(); i++){
			sinks.tryEmitNext(""+iss.charAt(i));
			Thread.sleep(1200);
		}

	}

}
