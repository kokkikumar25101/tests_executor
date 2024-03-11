import java.util.ArrayList;
import java.util.List;
"""
 design pattern should implement an api which will give list of all the subclasses implemented  in a different package to consolidate to 

"""
public class SubclassFinder {
  public static List<Class<?>> findSubclasses(String packageName) {
    List<Class<?>> subclasses = new ArrayList<>();
    Package pkg = Package.getPackage(packageName);
    if (pkg != null) {
      String packagePath = pkg.getName().replace('.', '/');
      try {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        java.net.URL resource = classLoader.getResource(packagePath);
        if (resource != null) {
          java.io.File packageDir = new java.io.File(resource.getFile());
          if (packageDir.exists()) {
            String[] files = packageDir.list();
            for (String file : files) {
              if (file.endsWith(".class")) {
                String className = packageName + '.' + file.substring(0, file.length() - 6);
                try {
                  Class<?> clazz = Class.forName(className);
                  if (!clazz.isInterface() && !clazz.isEnum() && !clazz.isAnnotation()) {
                    subclasses.add(clazz);
                  }
                } catch (ClassNotFoundException e) {
                  // Handle class not found exception
                }
              }
            }
          }
        }
      } catch (Exception e) {
        // Handle exception
      }
    }
    return subclasses;
  }
}