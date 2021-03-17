package qp.utils;

public class TypeChecker {
    public static boolean isType(Object object, Class<?>... classes) {
        for (Class<?> clazz : classes) {
            if (object.getClass().equals(clazz)) {
                return true;
            }
        }
        return false;
    }
}
