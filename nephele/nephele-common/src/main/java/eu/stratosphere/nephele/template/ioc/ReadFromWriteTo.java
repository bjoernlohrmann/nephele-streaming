package eu.stratosphere.nephele.template.ioc;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ReadFromWriteTo {
	int readerIndex();

	int[] writerIndices() default {};
}
