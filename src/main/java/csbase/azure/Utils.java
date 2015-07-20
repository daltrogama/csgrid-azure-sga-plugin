/**
 * $Id: Utils.java 163602 2015-04-14 18:56:26Z fpina $
 */
package csbase.azure;

import java.util.HashMap;
import java.util.Map;

import sgaidl.Pair;

/**
 * Utilitários.
 *
 * @author Tecgraf/PUC-Rio
 */
public class Utils {

  /**
   * Adiciona as entradas de um dicionário em um mapa.
   *
   * @param dictionary o odicionário
   * @param map o mapa
   */
  protected static void convertDicToMap(Pair[] dictionary,
    Map<String, String> map) {
    for (Pair pair : dictionary) {
      map.put(pair.key, pair.value);
    }
  }

  /**
   * Converte um dicionário para mapa.
   *
   * @param dictionary o dicionário
   *
   * @return o mapa
   */
  protected static Map<String, String> convertDicToMap(Pair[] dictionary) {
    Map<String, String> map = new HashMap<String, String>();
    if (dictionary != null)
	    for (Pair pair : dictionary) {
	      map.put(pair.key, pair.value);
	    }

    return map;
  }
}
