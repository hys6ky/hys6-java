package hyren.serv6.m.util.easyexcel;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ExcelValid {

    String message() default "";

    ValidRuleEnum[] rule() default { ValidRuleEnum.NOT_NULL };

    String[] regular() default {};
}
