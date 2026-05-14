package fruititem

type FruitItem struct {
	Fruit  string
	Amount uint32
}

func (fruitItem FruitItem) Sum(other FruitItem) FruitItem {
	return FruitItem{Fruit: fruitItem.Fruit, Amount: fruitItem.Amount + other.Amount}
}

func (fruitItem FruitItem) Less(other FruitItem) bool {
	return fruitItem.Amount < other.Amount
}
