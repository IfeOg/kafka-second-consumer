package com.example.lynxlaststand.model;

public class Client {

	private long id;

	private boolean crypto;

	private boolean gambling;

	private boolean atRisk;

	public Client() {
		super();
	}

	public Client(boolean crypto, boolean gambling, boolean atRisk) {
		this.crypto = crypto;
		this.gambling = gambling;
		this.atRisk = atRisk;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public boolean isCrypto() {
		return crypto;
	}

	public void setCrypto(boolean crypto) {
		this.crypto = crypto;
	}

	public boolean isGambling() {
		return gambling;
	}

	public void setGambling(boolean gambling) {
		this.gambling = gambling;
	}

	public boolean isAtRisk() {
		return atRisk;
	}

	public void setAtRisk(boolean atRisk) {
		this.atRisk = atRisk;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Client client = (Client) o;

		if (id != client.id) return false;
		if (crypto != client.crypto) return false;
		if (gambling != client.gambling) return false;
		return atRisk == client.atRisk;
	}

	@Override
	public int hashCode() {
		int result = (int) (id ^ (id >>> 32));
		result = 31 * result + (crypto ? 1 : 0);
		result = 31 * result + (gambling ? 1 : 0);
		result = 31 * result + (atRisk ? 1 : 0);
		return result;
	}

	@Override
	public String toString() {
		return "Client{" +
				"id=" + id +
				", crypto=" + crypto +
				", gambling=" + gambling +
				", atRisk=" + atRisk +
				'}';
	}
}
