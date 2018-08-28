package com.spark;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.apache.commons.codec.binary.Base64;

public class RSAUtils {

	/**
	 * 密钥算法RSA
	 */
	public static final String KEY_ALGORITHM = "RSA";

	/**
	 * 加密算法RSA
	 */
	public static final String CIPHER_ALGORITHM = "RSA/ECB/PKCS1PADDING";

	/**
	 * 签名算法
	 */
	public static final String SIGNATURE_ALGORITHM = "MD5withRSA";

	/**
	 * 获取公钥的key
	 */
	public static final String PUBLIC_KEY = "RSAPublicKey";

	/**
	 * 获取私钥的key
	 */
	public static final String PRIVATE_KEY = "RSAPrivateKey";

	/**
	 * RSA最大加密明文大小
	 */
	private static final int MAX_ENCRYPT_BLOCK = 117;

	/**
	 * RSA最大解密密文大小
	 */
	private static final int MAX_DECRYPT_BLOCK = 128;

	/**
	 * 生成密钥对(公钥和私钥)
	 */
	public static Map<String, Key> genKeyPair() {
		KeyPairGenerator keyPairGen;
		try {
			keyPairGen = KeyPairGenerator.getInstance(KEY_ALGORITHM);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
		keyPairGen.initialize(1024);
		KeyPair keyPair = keyPairGen.generateKeyPair();
		RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
		RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();
		Map<String, Key> keyMap = new HashMap<String, Key>(2);
		keyMap.put(PUBLIC_KEY, publicKey);
		keyMap.put(PRIVATE_KEY, privateKey);
		return keyMap;
	}

	

	/**
	 * 私钥解密
	 * 
	 * @param encryptedData
	 *            已加密数据
	 * @param privateKey
	 *            私钥(BASE64编码)
	 * @throws InvalidKeySpecException 
	 * @throws NoSuchAlgorithmException 
	 * @throws NoSuchPaddingException 
	 * @throws InvalidKeyException 
	 * @throws BadPaddingException 
	 * @throws IllegalBlockSizeException 
	 * @throws UnsupportedEncodingException 
	 */
	public static String decryptByPrivateKey(String srcData, String privateKey) throws InvalidKeySpecException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException, UnsupportedEncodingException  {
		ByteArrayOutputStream out = null;
		try {
			byte[] keyBytes =Base64.decodeBase64(privateKey.getBytes());
			byte[] encryptedData=Base64.decodeBase64(srcData);
			PKCS8EncodedKeySpec pkcs8KeySpec = new PKCS8EncodedKeySpec(keyBytes);
			KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
			Key privateK = keyFactory.generatePrivate(pkcs8KeySpec);
			Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
			cipher.init(Cipher.DECRYPT_MODE, privateK);
			int inputLen = encryptedData.length;
			out = new ByteArrayOutputStream();
			int offSet = 0;
			byte[] cache;
			int i = 0;
			// 对数据分段解密
			while (inputLen - offSet > 0) {
				if (inputLen - offSet > MAX_DECRYPT_BLOCK) {
					cache = cipher.doFinal(encryptedData, offSet, MAX_DECRYPT_BLOCK);
				} else {
					cache = cipher.doFinal(encryptedData, offSet, inputLen - offSet);
				}
				out.write(cache, 0, cache.length);
				i++;
				offSet = i * MAX_DECRYPT_BLOCK;
			}
			byte[] decryptedData = out.toByteArray();
			return new String(decryptedData,"utf-8");
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 公钥解密
	 * 
	 * @param encryptedData
	 *            已加密数据
	 * @param publicKey
	 *            公钥(BASE64编码)
	 * @throws NoSuchAlgorithmException 
	 * @throws InvalidKeySpecException 
	 * @throws NoSuchPaddingException 
	 * @throws InvalidKeyException 
	 * @throws BadPaddingException 
	 * @throws IllegalBlockSizeException 
	 * @throws UnsupportedEncodingException 
	 */
	public static String decryptByPublicKey(String srcData, String publicKey) throws NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException, UnsupportedEncodingException  {
		ByteArrayOutputStream out = null;
		try {
			byte[] keyBytes = Base64.decodeBase64(publicKey.getBytes());
			byte[] encryptedData=Base64.decodeBase64(srcData.getBytes());
			X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
			KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
			Key publicK = keyFactory.generatePublic(x509KeySpec);
			Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
			cipher.init(Cipher.DECRYPT_MODE, publicK);
			int inputLen = encryptedData.length;
			out = new ByteArrayOutputStream();
			int offSet = 0;
			byte[] cache;
			int i = 0;
			// 对数据分段解密
			while (inputLen - offSet > 0) {
				if (inputLen - offSet > MAX_DECRYPT_BLOCK) {
					cache = cipher.doFinal(encryptedData, offSet, MAX_DECRYPT_BLOCK);
				} else {
					cache = cipher.doFinal(encryptedData, offSet, inputLen - offSet);
				}
				out.write(cache, 0, cache.length);
				i++;
				offSet = i * MAX_DECRYPT_BLOCK;
			}
			byte[] decryptedData = out.toByteArray();
			return new String(decryptedData,"utf-8");
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 公钥加密
	 * 
	 * @param data
	 *            源数据
	 * @param publicKey
	 *            公钥(BASE64编码)
	 * @throws NoSuchAlgorithmException 
	 * @throws InvalidKeySpecException 
	 * @throws NoSuchPaddingException 
	 * @throws InvalidKeyException 
	 * @throws BadPaddingException 
	 * @throws IllegalBlockSizeException 
	 * @throws UnsupportedEncodingException 
	 */
	public static String encryptByPublicKey(String srcData, String publicKey) throws NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException, UnsupportedEncodingException  {
		ByteArrayOutputStream out = null;
		try {
			byte[] keyBytes =Base64.decodeBase64(publicKey.getBytes());
			byte[] data=srcData.getBytes("utf-8");
			X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
			KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
			Key publicK = keyFactory.generatePublic(x509KeySpec);
			// 对数据加密
			Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
			cipher.init(Cipher.ENCRYPT_MODE, publicK);
			int inputLen = data.length;
			out = new ByteArrayOutputStream();
			int offSet = 0;
			byte[] cache;
			int i = 0;
			// 对数据分段加密
			while (inputLen - offSet > 0) {
				if (inputLen - offSet > MAX_ENCRYPT_BLOCK) {
					cache = cipher.doFinal(data, offSet, MAX_ENCRYPT_BLOCK);
				} else {
					cache = cipher.doFinal(data, offSet, inputLen - offSet);
				}
				out.write(cache, 0, cache.length);
				i++;
				offSet = i * MAX_ENCRYPT_BLOCK;
			}
			byte[] encryptedData = out.toByteArray();
			return  Base64.encodeBase64String(encryptedData);
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 私钥加密
	 * 
	 * @param data
	 *            源数据
	 * @param privateKey
	 *            私钥(BASE64编码)
	 * @throws NoSuchAlgorithmException 
	 * @throws InvalidKeySpecException 
	 * @throws NoSuchPaddingException 
	 * @throws InvalidKeyException 
	 * @throws BadPaddingException 
	 * @throws IllegalBlockSizeException 
	 * @throws UnsupportedEncodingException 
	 */
	public static String encryptByPrivateKey(String srcData, String privateKey) throws NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException, UnsupportedEncodingException  {
		ByteArrayOutputStream out = null;
		try {
			byte[] keyBytes = Base64.decodeBase64(privateKey.getBytes());
			byte[] data=srcData.getBytes("utf-8");
			PKCS8EncodedKeySpec pkcs8KeySpec = new PKCS8EncodedKeySpec(keyBytes);
			KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
			Key privateK = keyFactory.generatePrivate(pkcs8KeySpec);
			Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
			cipher.init(Cipher.ENCRYPT_MODE, privateK);
			int inputLen = data.length;
			out = new ByteArrayOutputStream();
			int offSet = 0;
			byte[] cache;
			int i = 0;
			// 对数据分段加密
			while (inputLen - offSet > 0) {
				if (inputLen - offSet > MAX_ENCRYPT_BLOCK) {
					cache = cipher.doFinal(data, offSet, MAX_ENCRYPT_BLOCK);
				} else {
					cache = cipher.doFinal(data, offSet, inputLen - offSet);
				}
				out.write(cache, 0, cache.length);
				i++;
				offSet = i * MAX_ENCRYPT_BLOCK;
			}
			byte[] encryptedData = out.toByteArray();
			return Base64.encodeBase64String(encryptedData);
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	

	/**
	 * 获取私钥
	 * 
	 * @param keyMap
	 *            密钥对
	 */
	public static String getPrivateKey(Map<String, Key> keyMap) {
		Key key = keyMap.get(PRIVATE_KEY);
		return  Base64.encodeBase64String(key.getEncoded());
	}

	/**
	 * 获取公钥
	 * 
	 * @param keyMap
	 *            密钥对
	 */
	public static String getPublicKey(Map<String, Key> keyMap) {
		Key key = keyMap.get(PUBLIC_KEY);
		
		System.out.println("转换前长度" + key.getEncoded().length);
		
//		return Base64Utils.encode(key.getEncoded());
		return Base64.encodeBase64String(key.getEncoded());
	}
	
	
 
	
	public static void main(String[] args) throws Exception {	
		//Map<String,Key> keymap=genKeyPair();
		
		String privateKey =  "MIICdQIBADANBgkqhkiG9w0BAQEFAASCAl8wggJbAgEAAoGBAJJAwPFC1uhxOoohbhr3YjYxJ3NDR17lsm4FkFsr03yLnF+nBZxCxn1hBiecf0j2OukRz3oCFt/fEQdiUvlOOMgfPF+JcDOWmhKkGxdq4l0W4vPclfJA2+sS+72+T5OyzMlsqKdoCv+TAX1zKS7mMQULe9KIUVI07HqYM4YzvsRVAgMBAAECgYA5AjizUoSG8qwI7+MFa+zChwKDsXP+j7avoEeW4kx7vcfkmSxcrsLEWK+XfS84d5KbCzA+tNXJyh/PKwqA13bWIfMOcVscazKdS5zh0HYmHE8/UehKhm1vk8eIaFISInkiStXsG9k3dcGe+wM0oXBS1xntX4NGylTN+NtuPSJWeQJBAObRgLnOEqQ6SPaHLvemDVAtQiwp44kUZEisVs+Ns8AbUVNthmJRG/PlI6o1bk137eCGtX2E6m4+ywh6JiTFYAcCQQCiNW8H+gQu2fXouILNYtjhfh7eLrlKXncBPCO7fAC7UuHII0TrnihWNMZeH/088JrUzt43Gvz0ZiG+47vObqnDAkBLEgp+4/IWLcq7O55f90bPM9kYygrx84rmQ/78BEdZDMl3i+CwK1cfDQB7hGM6mO3qH4X5q/gfIRchy7CKNxOTAkBD+lKqrBteqxdETTb77eXoMhurjzc1tHr4+IQdCS2hU22tmyJCAAj1f652Ob+97zNj5cH4pAXIQpj3z6agL35TAkBjUf4J7tWaAXKmTpop6MnHcb10SoyF/XZM2xPY8vkyhdboOfc7QNcro9dlTRm4v40ofHH8GVluLls93317Guvk";
				//Base64.encodeBase64String(keymap.get(PRIVATE_KEY).getEncoded());
		String publicKey = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCSQMDxQtbocTqKIW4a92I2MSdzQ0de5bJuBZBbK9N8i5xfpwWcQsZ9YQYnnH9I9jrpEc96Ahbf3xEHYlL5TjjIHzxfiXAzlpoSpBsXauJdFuLz3JXyQNvrEvu9vk+TsszJbKinaAr/kwF9cyku5jEFC3vSiFFSNOx6mDOGM77EVQIDAQAB";
		//Base64.encodeBase64String(keymap.get(PUBLIC_KEY).getEncoded());
//		System.out.println(privateKey.length());
//		System.out.println(publicKey.length());                                            
		System.out.println("Public Key:" + publicKey);
		System.out.println("Private Key:" + privateKey);
//		String v = "{\"shop_code\":\"1001\",\"parameters\":[\"remain_count\"]}";
		String v = "{\"shopCode\":\"test1\",\"produceId\":10,\"joinPointId\":1234,\"templateContent\":\"您好${name},欢迎光临222！\"}";
//		String v = "bvV72GqGobmISjNobqvgd+4wwwvvSbf1CwvaijgX5Ktu52Bk0n/4ptuY5DY/oua0MrxViGrUlnJi9nqXLnx4le+jvXCB57QhpLKPo4Qd5XndVKGapM8s9d6I5DkLYbsFjAm2jHlX+dvtADFvV1TSOX4ozAG/7J3VTBR+xC4AdgY=";
//		String ensrc=encryptByPrivateKey(v,privateKey);
//		String desrc=decryptByPublicKey(v,publicKey);
//		System.out.println(ensrc);
//		System.out.println(desrc);
		
//		System.out.println("------------------------");
//		
//		
//		ensrc=encryptByPublicKey(v,publicKey);
//		String desrc=decryptByPrivateKey(v,privateKey);
//		System.out.println(encryptByPublicKey(v,publicKey));
//		System.out.println(desrc);
	}
}