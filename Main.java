package dataclean;

import dataclean.interaction.Client;

import java.io.IOException;

public class Main {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Client c = new Client(args[0]);
		c.run();
	}
}
